package textsplitter

import (
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/tmc/langchaingo/schema"
)

func TestMarkdownHeaderTextSplitter_SplitText(t *testing.T) {
	t.Parallel()

	type testCase struct {
		markdown     string
		expectedDocs []schema.Document
	}

	testCases := []testCase{
		{
			markdown: `
### This is a header

- This is a list item of bullet type.
- This is another list item.

 *Everything* is going according to **plan**.
`,
			expectedDocs: []schema.Document{
				{
					PageContent: `### This is a header
- This is a list item of bullet type.
`,
					Metadata: map[string]any{},
				},
				{
					PageContent: `### This is a header
- This is another list item.
`,
					Metadata: map[string]any{},
				},
				{
					PageContent: `### This is a header
*Everything* is going according to **plan**.
`,
					Metadata: map[string]any{},
				},
			},
		},
		{
			markdown: "example code:\n```go\nfunc main() {}\n```\n",
			expectedDocs: []schema.Document{
				{PageContent: "example code:\n```go\nfunc main() {}\n```\n", Metadata: map[string]any{}},
			},
		},
	}

	splitter := NewMarkdownTextSplitter(WithChunkSize(64), WithChunkOverlap(32))
	for _, tc := range testCases {
		docs, err := CreateDocuments(splitter, []string{tc.markdown}, nil)
		assert.NoError(t, err)
		assert.Equal(t, tc.expectedDocs, docs)
	}
}

// TestMarkdownHeaderTextSplitter_Table markdown always split by line.
func TestMarkdownHeaderTextSplitter_Table(t *testing.T) {
	t.Parallel()
	type testCase struct {
		markdown     string
		expectedDocs []schema.Document
	}
	testCases := []testCase{
		{
			markdown: `| Syntax      | Description |
| ----------- | ----------- |
| Header      | Title       |
| Paragraph   | Text        |`,
			expectedDocs: []schema.Document{
				{
					PageContent: `| Syntax | Description |
| --- | --- |
| Header | Title |`,
					Metadata: map[string]any{},
				},
				{
					PageContent: `| Syntax | Description |
| --- | --- |
| Paragraph | Text |`,
					Metadata: map[string]any{},
				},
			},
		},
	}

	for _, tc := range testCases {
		splitter := NewMarkdownTextSplitter(WithChunkSize(64), WithChunkOverlap(32))
		docs, err := CreateDocuments(splitter, []string{tc.markdown}, nil)
		assert.NoError(t, err)
		assert.Equal(t, tc.expectedDocs, docs)

		splitter = NewMarkdownTextSplitter(WithChunkSize(512), WithChunkOverlap(64))
		docs, err = CreateDocuments(splitter, []string{tc.markdown}, nil)
		assert.NoError(t, err)
		assert.Equal(t, tc.expectedDocs, docs)
	}
}

func TestMarkdownHeaderTextSplitter(t *testing.T) {
	t.Parallel()

	data, err := os.ReadFile("./testdata/example.md")
	if err != nil {
		t.Fatal(err)
	}

	splitter := NewMarkdownTextSplitter(WithChunkSize(512), WithChunkOverlap(64))
	docs, err := CreateDocuments(splitter, []string{string(data)}, nil)
	if err != nil {
		t.Fatal(err)
	}

	var pages string
	for _, doc := range docs {
		pages += doc.PageContent + "\n\n---\n\n"
	}

	err = os.WriteFile("./testdata/example_markdown_header_512.md", []byte(pages), os.ModeExclusive|os.ModePerm)
	if err != nil {
		t.Fatal(err)
	}
}

func TestMarkdownHeaderTextSplitter_BulletList(t *testing.T) {
	t.Parallel()
	type testCase struct {
		markdown     string
		expectedDocs []schema.Document
	}
	testCases := []testCase{
		{
			markdown: `
- [Code of Conduct](#code-of-conduct)
- [I Have a Question](#i-have-a-question)
- [I Want To Contribute](#i-want-to-contribute)
    - [Reporting Bugs](#reporting-bugs)
        - [Before Submitting a Bug Report](#before-submitting-a-bug-report)
        - [How Do I Submit a Good Bug Report?](#how-do-i-submit-a-good-bug-report)
    - [Suggesting Enhancements](#suggesting-enhancements)
        - [Before Submitting an Enhancement](#before-submitting-an-enhancement)
        - [How Do I Submit a Good Enhancement Suggestion?](#how-do-i-submit-a-good-enhancement-suggestion)
    - [Your First Code Contribution](#your-first-code-contribution)
        - [Make Changes](#make-changes)
            - [Make changes in the UI](#make-changes-in-the-ui)
            - [Make changes locally](#make-changes-locally)
        - [Commit your update](#commit-your-update)
        - [Pull Request](#pull-request)
        - [Your PR is merged!](#your-pr-is-merged)
`,
			expectedDocs: []schema.Document{
				{
					PageContent: `- [Code of Conduct](#code-of-conduct)
- [I Have a Question](#i-have-a-question)
`,
					Metadata: map[string]any{},
				},
				{
					PageContent: `- [I Want To Contribute](#i-want-to-contribute)
  - [Reporting Bugs](#reporting-bugs)
    - [Before Submitting a Bug Report](#before-submitting-a-bug-report)
    - [How Do I Submit a Good Bug Report?](#how-do-i-submit-a-good-bug-report)
  - [Suggesting Enhancements](#suggesting-enhancements)
    - [Before Submitting an Enhancement](#before-submitting-an-enhancement)
    - [How Do I Submit a Good Enhancement Suggestion?](#how-do-i-submit-a-good-enhancement-suggestion)
`,
					Metadata: map[string]any{},
				},
				{
					PageContent: `  - [Your First Code Contribution](#your-first-code-contribution)
    - [Make Changes](#make-changes)
      - [Make changes in the UI](#make-changes-in-the-ui)
      - [Make changes locally](#make-changes-locally)
    - [Commit your update](#commit-your-update)
    - [Pull Request](#pull-request)
    - [Your PR is merged!](#your-pr-is-merged)
`,
					Metadata: map[string]any{},
				},
			},
		},
	}

	for _, tc := range testCases {
		splitter := NewMarkdownTextSplitter(WithChunkSize(512), WithChunkOverlap(64))
		docs, err := CreateDocuments(splitter, []string{tc.markdown}, nil)
		assert.NoError(t, err)
		assert.Equal(t, tc.expectedDocs, docs)
	}
}

func TestMarkdownHeaderTextSplitter_HeaderAfterHeader(t *testing.T) {
	t.Parallel()

	type testCase struct {
		markdown     string
		expectedDocs []schema.Document
	}

	testCases := []testCase{
		{
			markdown: `
### Your First Code Contribution

#### Make Changes

##### Make changes in the UI

Click **Make a contribution** at the bottom of any docs page to make small changes such as a typo, sentence fix, or a
broken link. This takes you to the .md file where you can make your changes and [create a pull request](#pull-request)
for a review.

##### Make changes locally

1. Fork the repository.

2. Install or make sure **Golang** is updated.

3. Create a working branch and start with your changes!
`,
			expectedDocs: []schema.Document{
				{
					PageContent: `### Your First Code Contribution
`, Metadata: map[string]any{},
				},
				{
					PageContent: `#### Make Changes
`, Metadata: map[string]any{},
				},
				{
					PageContent: `##### Make changes in the UI
Click **Make a contribution** at the bottom of any docs page to make small changes such as a typo, sentence fix, or a
broken link. This takes you to the .md file where you can make your changes and [create a pull request](#pull-request)
for a review.
`, Metadata: map[string]any{},
				},
				{
					PageContent: `##### Make changes locally
1. Fork the repository.
2. Install or make sure **Golang** is updated.
3. Create a working branch and start with your changes!
`, Metadata: map[string]any{},
				},
			},
		},
	}

	for _, tc := range testCases {
		splitter := NewMarkdownTextSplitter(WithChunkSize(512), WithChunkOverlap(64))
		docs, err := CreateDocuments(splitter, []string{tc.markdown}, nil)
		assert.NoError(t, err)
		assert.Equal(t, tc.expectedDocs, docs)
	}
}

func TestMarkdownHeaderTextSplitters_Metadata(t *testing.T) {
	t.Parallel()

	type testCaseDef struct {
		name           string
		markdown       string
		expectedChunks []Chunk
	}

	type meta map[string]any

	testCases := []testCaseDef{
		{
			name: "First case",
			// nolint:dupword
			markdown: getTestCaseMarkdownWithMetadata(),
			expectedChunks: []Chunk{
				{Text: "### __A\n__A Contents.\n", Metadata: meta{"h3": "__A"}},
				{Text: "#### __AA\n", Metadata: meta{"h3": "__A", "h4": "__AA"}},
				{Text: "##### __AAA\n", Metadata: meta{"h3": "__A", "h4": "__AA", "h5": "__AAA"}},
				{Text: "## _A\n", Metadata: meta{"h2": "_A"}},
				{Text: "### _AA\n", Metadata: meta{"h2": "_A", "h3": "_AA"}},
				{Text: "## _B\n_B Contents.\n", Metadata: meta{"h2": "_B"}},
				{Text: "### _BA\n_BA Contents.\n", Metadata: meta{"h2": "_B", "h3": "_BA"}},
				{Text: "# A\nA Contents.\n", Metadata: meta{"h1": "A"}}, //nolint:dupword
				{Text: "## AA\n```\n" +
					"# This Code comment should not be counted as a header\n## Neither should this one\n" +
					"# Or this one\n```\nAA Contents come here.\n", Metadata: meta{"h1": "A", "h2": "AA"}},
				{Text: "## AB\nAB Contents.\n", Metadata: meta{"h1": "A", "h2": "AB"}}, //nolint:dupword
				{Text: "# C\nAnd finally C's Contents.\n", Metadata: meta{"h1": "C"}},
			},
		},
	}

	levelHeaderFn := func(level int, text string) map[string]any {
		if text == "" {
			return nil
		}
		return map[string]any{fmt.Sprintf("h%d", level): text}
	}

	markdownSplitters := []TextSplitter{
		NewMarkdownTextSplitter(WithLevelHeaderFn(levelHeaderFn)),
		NewMarkdownTextSplitterV2(WithLevelHeaderFn(levelHeaderFn)),
	}

	for _, tc := range testCases {
		tc := tc
		for _, ms := range markdownSplitters {
			ms := ms
			t.Run(fmt.Sprintf("%s: %T", tc.name, ms), func(t *testing.T) {
				t.Parallel()
				chunks, err := ms.SplitText(tc.markdown)
				assert.NoError(t, err)
				// fmt.Printf("%#v\n", chunks)
				assert.Equal(t, tc.expectedChunks, chunks)
				reconstructedMarkdown := reconstructMarkupFn(chunks)
				assert.Equal(t, tc.markdown, reconstructedMarkdown)
			})
		}
	}
}

//nolint:dupword
func getTestCaseMarkdownWithMetadata() string {
	return strings.Join([]string{
		`### __A
__A Contents.
#### __AA
##### __AAA
## _A
### _AA
## _B
_B Contents.
### _BA
_BA Contents.
# A
A Contents.
## AA
`, "```", `
# This Code comment should not be counted as a header
## Neither should this one
# Or this one
`, "```", `
AA Contents come here.
## AB
AB Contents.
# C
And finally C's Contents.
`,
	}, "")
}

func reconstructMarkupFn(chunks []Chunk) string {
	texts := make([]string, len(chunks))
	for i := range chunks {
		texts[i] = chunks[i].Text
	}
	return strings.Join(texts, "")
}
