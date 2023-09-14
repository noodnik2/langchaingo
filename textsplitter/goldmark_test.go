package textsplitter

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGoldMark(t *testing.T) {
	t.Parallel()

	markdown := `
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

### Code Conduct

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

### Table example

| Syntax      | Description |
| ----------- | ----------- |
| Header      | Title       |
| Paragraph   | Text        |

`

	type meta = map[string]any
	expectedChunks := []Chunk{
		{Text: "### Your First Code Contribution\n", Metadata: meta{"hdr_title": "Your First Code Contribution"}},
		{Text: "#### Make Changes\n", Metadata: meta{"hdr_detail": "Make Changes",
			"hdr_title": "Your First Code Contribution"}},
		{Text: "##### Make changes in the UI\nClick **Make a contribution** at the bottom of any docs page" +
			" to make small changes such as a typo, sentence fix, or a\nbroken link. This takes you to the .md" +
			" file where you can make your changes and [create a pull request](#pull-request)\nfor a review.\n",
			Metadata: meta{"hdr_detail": "Make Changes", "hdr_title": "Your First Code Contribution"}},
		{Text: "##### Make changes locally\n1. Fork the repository.\n2. Install or make sure **Golang** is updated.\n" +
			"3. Create a working branch and start with your changes!\n", Metadata: meta{"hdr_detail": "Make Changes",
			"hdr_title": "Your First Code Contribution"}},
		{Text: "### Code Conduct\n- [Code of Conduct](#code-of-conduct)\n- [I Have a Question](#i-have-a-question)\n",
			Metadata: meta{"hdr_title": "Code Conduct"}},
		{Text: "### Code Conduct\n- [I Want To Contribute](#i-want-to-contribute)\n" +
			"  - [Reporting Bugs](#reporting-bugs)\n" +
			"    - [Before Submitting a Bug Report](#before-submitting-a-bug-report)\n" +
			"    - [How Do I Submit a Good Bug Report?](#how-do-i-submit-a-good-bug-report)\n" +
			"  - [Suggesting Enhancements](#suggesting-enhancements)\n" +
			"    - [Before Submitting an Enhancement](#before-submitting-an-enhancement)\n" +
			"    - [How Do I Submit a Good Enhancement Suggestion?](#how-do-i-submit-a-good-enhancement-suggestion)\n",
			Metadata: meta{"hdr_title": "Code Conduct"}},
		{Text: "### Code Conduct\n  - [Your First Code Contribution](#your-first-code-contribution)\n" +
			"    - [Make Changes](#make-changes)\n" +
			"      - [Make changes in the UI](#make-changes-in-the-ui)\n" +
			"      - [Make changes locally](#make-changes-locally)\n" +
			"    - [Commit your update](#commit-your-update)\n    - [Pull Request](#pull-request)\n" +
			"    - [Your PR is merged!](#your-pr-is-merged)\n", Metadata: meta{"hdr_title": "Code Conduct"}},
		{Text: "### Table example\n| Syntax | Description |\n| --- | --- |\n| Header | Title |",
			Metadata: meta{"hdr_title": "Table example"}},
		{Text: "### Table example\n| Syntax | Description |\n| --- | --- |\n| Paragraph | Text |",
			Metadata: meta{"hdr_title": "Table example"}},
	}

	levelHeaderFn := func(level int, text string) map[string]any {
		if text != "" {
			knownHeaderLevels := map[int]string{3: "hdr_title", 4: "hdr_detail"}
			if hdrName := knownHeaderLevels[level]; hdrName != "" {
				return map[string]any{hdrName: text}
			}
		}
		return nil
	}

	testCases := []TextSplitter{
		NewMarkdownTextSplitter(WithLevelHeaderFn(levelHeaderFn)),
		NewMarkdownTextSplitterV2(WithLevelHeaderFn(levelHeaderFn)),
	}

	for _, sp := range testCases {
		sp := sp
		t.Run(fmt.Sprintf("%T", sp), func(t *testing.T) {
			t.Parallel()

			actualChunks, err := sp.SplitText(markdown)
			assert.NoError(t, err)

			assert.Equal(t, expectedChunks, actualChunks)
			for _, chunk := range actualChunks {
				fmt.Printf("%#v\n", chunk)
			}
		})
	}
}
