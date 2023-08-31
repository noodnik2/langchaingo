package textsplitter

import (
	"fmt"
	"strings"
	"unicode/utf8"

	"github.com/yuin/goldmark"
	"github.com/yuin/goldmark/ast"
	"github.com/yuin/goldmark/text"
	"github.com/yuin/goldmark/util"
)

// NewMarkdownTextSplitterV2 creates a new MarkdownTextSplitterV2
func NewMarkdownTextSplitterV2(opts ...Option) MarkdownTextSplitterV2 {
	options := DefaultOptions()
	for _, o := range opts {
		o(&options)
	}

	sp := MarkdownTextSplitterV2{
		ChunkSize:      options.ChunkSize,
		ChunkOverlap:   options.ChunkOverlap,
		SecondSplitter: options.SecondSplitter,
	}

	sp.nodeRenderers = map[ast.NodeKind]NodeRender{
		ast.KindDocument:  sp.renderDocument,
		ast.KindHeading:   sp.renderHeading,
		ast.KindParagraph: sp.renderParagraph,
		ast.KindList:      sp.renderList,
		ast.KindListItem:  sp.renderListItem,
		ast.KindEmphasis:  sp.renderEmphasis,
		ast.KindTextBlock: sp.renderTextBlock,
		ast.KindAutoLink:  sp.renderAutoLink,

		// inlines
		ast.KindLink:   sp.renderLink,
		ast.KindText:   sp.renderText,
		ast.KindString: sp.renderString,
	}

	if sp.SecondSplitter == nil {
		sp.SecondSplitter = NewRecursiveCharacter(
			WithChunkSize(options.ChunkSize),
			WithChunkOverlap(options.ChunkOverlap),
			WithSeparators([]string{
				"\n\n", // new line
				"\n",   // new line
				" ",    // space
			}),
		)
	}

	return sp
}

// MarkdownTextSplitterV2 Markdown text splitter
type MarkdownTextSplitterV2 struct {
	ChunkSize    int
	ChunkOverlap int
	// SecondSplitter splits paragraphs
	SecondSplitter TextSplitter

	// nodeRenderers is a map of node kind and NodeRender.
	nodeRenderers map[ast.NodeKind]NodeRender
}

// SplitText splits a text into multiple text.
func (m *MarkdownTextSplitterV2) SplitText(s string) ([]string, error) {
	reader := text.NewReader([]byte(s))

	node := goldmark.DefaultParser().Parse(reader)

	node.Dump(reader.Source(), 0)

	writer := &MarkdownWriter{
		chunkSize:      m.ChunkSize,
		chunkOverlap:   m.ChunkOverlap,
		secondSplitter: m.SecondSplitter,
	}

	err := m.Render(writer, node, []byte(s))
	if err != nil {
		return nil, err
	}

	if err = writer.Flush(); err != nil {
		return nil, err
	}

	return writer.chunks, nil
}

// =================================================================================================================//
// =================================================================================================================//
// =================================================================================================================//
// =================================================================================================================//
// =================================================================================================================//
// =================================================================================================================//

// NodeRender is a function that renders a markdown node.
type NodeRender = func(w util.BufWriter, source []byte, node ast.Node, entering bool) (ast.WalkStatus, error)

// Render renders a markdown node.
func (m *MarkdownTextSplitterV2) Render(writer util.BufWriter, n ast.Node, source []byte) error {
	err := ast.Walk(n, func(n ast.Node, entering bool) (ast.WalkStatus, error) {
		s := ast.WalkContinue
		var err error
		if f := m.nodeRenderers[n.Kind()]; f != nil {
			s, err = f(writer, source, n, entering)
		}
		return s, err
	})

	return err
}

// renderDocument renders a markdown document root node.
func (m *MarkdownTextSplitterV2) renderDocument(
	util.BufWriter, []byte, ast.Node, bool,
) (ast.WalkStatus, error) {
	return ast.WalkContinue, nil
}

// renderHeading renders a heading node.
func (m *MarkdownTextSplitterV2) renderHeading(
	w util.BufWriter, source []byte, node ast.Node, entering bool,
) (ast.WalkStatus, error) {
	n, _ := node.(*ast.Heading)
	writer, _ := w.(*MarkdownWriter)

	if !entering {
		_, _ = w.WriteString("\n")
		return ast.WalkContinue, nil
	}

	_, _ = w.WriteString(strings.Repeat("#", n.Level))
	_, _ = w.WriteString(" ")
	_, _ = w.Write(n.Text(source))

	writer.hTitlePrepended = false
	return ast.WalkSkipChildren, nil
}

// renderParagraph renders a paragraph node.
func (m *MarkdownTextSplitterV2) renderParagraph(
	w util.BufWriter, source []byte, node ast.Node, entering bool,
) (ast.WalkStatus, error) {
	if !entering {
		fmt.Printf("paragraph leaving: %s\n", node.Text(source))
		_, _ = w.WriteString("\n\n")
		return ast.WalkContinue, nil
	}

	return ast.WalkContinue, nil
}

// renderList renders a list node.
func (m *MarkdownTextSplitterV2) renderList(
	w util.BufWriter, source []byte, node ast.Node, entering bool,
) (ast.WalkStatus, error) {
	n, _ := node.(*ast.List)
	writer, _ := w.(*MarkdownWriter)

	if !entering {
		fmt.Printf("list leaving: %s\n", node.Text(source))

		_, _ = w.WriteString("\n")
		return ast.WalkContinue, nil
	}

	nnw := writer.clone()
	if n.IsOrdered() {
		nnw.orderedList = true
	} else {
		nnw.bulletList = true
	}

	_, _ = w.WriteString("\n")

	for c := n.FirstChild(); c != nil; c = c.NextSibling() {
		nnw.indentLevel = writer.indentLevel
		nnw.indentLevel++

		err := m.Render(nnw, c, source)
		if err != nil {
			return ast.WalkStop, err
		}

		nnw.flush()

		for i := range nnw.chunks {
			if nnw.indentLevel > 1 {
				nnw.chunks[i] = formatWithIndent(nnw.chunks[i], "  ")
			}
		}

		_, _ = writer.WriteString(strings.Join(nnw.chunks, "\n"))

		nnw.chunks = []string{}
	}

	return ast.WalkSkipChildren, nil
}

// renderListItem renders a list item node.
func (m *MarkdownTextSplitterV2) renderListItem(
	w util.BufWriter, source []byte, node ast.Node, entering bool,
) (ast.WalkStatus, error) {
	if !entering {
		_, _ = w.WriteString("\n")

		fmt.Printf("list item leaving: %s\n", node.Text(source))

		return ast.WalkContinue, nil
	}

	writer, _ := w.(*MarkdownWriter)
	n, _ := node.(*ast.ListItem)
	if n.Parent().Kind() == ast.KindList {
		writer.listOrder++
	}
	if writer.orderedList {
		_, _ = writer.WriteString(fmt.Sprintf("%d. ", writer.listOrder))
	} else {
		_, _ = writer.WriteString("- ")
	}

	return ast.WalkContinue, nil
}

// renderEmphasis renders an emphasis node.
func (m *MarkdownTextSplitterV2) renderEmphasis(
	w util.BufWriter, _ []byte, _ ast.Node, entering bool,
) (ast.WalkStatus, error) {
	if !entering {
		_, _ = w.WriteString("**")
		return ast.WalkContinue, nil
	}

	_, _ = w.WriteString("**")
	return ast.WalkContinue, nil
}

// renderTextBlock renders a text block node.
func (m *MarkdownTextSplitterV2) renderTextBlock(
	w util.BufWriter, _ []byte, node ast.Node, entering bool,
) (ast.WalkStatus, error) {
	if !entering {
		if _, ok := node.NextSibling().(ast.Node); ok && node.FirstChild() != nil {
			_ = w.WriteByte('\n')
		}
	}

	return ast.WalkContinue, nil
}

// renderAutoLink renders an auto link node.
func (m *MarkdownTextSplitterV2) renderAutoLink(
	w util.BufWriter, source []byte, node ast.Node, entering bool,
) (ast.WalkStatus, error) {
	n, _ := node.(*ast.AutoLink)
	if !entering {
		return ast.WalkContinue, nil
	}

	_, _ = w.WriteString(fmt.Sprintf("[%s](%s)", n.Label(source), n.URL(source)))

	return ast.WalkContinue, nil
}

// renderLink renders a link node.
func (m *MarkdownTextSplitterV2) renderLink(
	w util.BufWriter, source []byte, node ast.Node, entering bool,
) (ast.WalkStatus, error) {
	n, _ := node.(*ast.Link)
	if !entering {
		return ast.WalkContinue, nil
	}

	_, _ = w.WriteString(fmt.Sprintf("[%s](%s)", n.Title, n.Destination))

	return ast.WalkContinue, nil
}

// renderText renders a text node.
func (m *MarkdownTextSplitterV2) renderText(
	w util.BufWriter, source []byte, node ast.Node, entering bool,
) (ast.WalkStatus, error) {
	if !entering {
		return ast.WalkContinue, nil
	}

	n, _ := node.(*ast.Text)
	segment := n.Segment
	if _, err := w.Write(segment.Value(source)); err != nil {
		return ast.WalkStop, err
	}

	return ast.WalkContinue, nil
}

// renderString renders a string node.
func (m *MarkdownTextSplitterV2) renderString(
	w util.BufWriter, source []byte, node ast.Node, entering bool,
) (ast.WalkStatus, error) {
	if !entering {
		fmt.Printf("string leaving: %s\n", node.Text(source))
		return ast.WalkContinue, nil
	}

	n, _ := node.(*ast.String)
	if _, err := w.Write(n.Value); err != nil {
		return ast.WalkStop, err
	}

	return ast.WalkContinue, nil
}

// =================================================================================================================//
// =================================================================================================================//
// =================================================================================================================//
// =================================================================================================================//
// =================================================================================================================//
// =================================================================================================================//

// MarkdownWriter writes Markdown text to chunks
type MarkdownWriter struct {
	// hTitle represents the current header(H1、H2 etc.) content
	hTitle string
	// hTitlePrepended represents whether hTitle has been appended to chunks
	hTitlePrepended bool

	// orderedList represents whether current list is ordered list
	orderedList bool
	// bulletList represents whether current list is bullet list
	bulletList bool
	// listOrder represents the current order number for ordered list
	listOrder int

	// indentLevel represents the current indent level for ordered、unordered lists
	indentLevel int

	curSnippet   string
	chunkSize    int
	chunkOverlap int
	chunks       []string
	// secondSplitter re-split markdown single long paragraph into chunks
	secondSplitter TextSplitter
}

// Available returns the number of bytes that can be written without blocking.
func (m *MarkdownWriter) Available() int {
	return m.chunkSize
}

// Buffered returns the number of bytes that have been written into the current chunk.
func (m *MarkdownWriter) Buffered() int {
	return len(m.curSnippet)
}

// Flush writes any buffered data to the underlying io.Writer.
func (m *MarkdownWriter) Flush() error {
	m.flush()
	return nil
}

// WriteByte writes a single byte.
func (m *MarkdownWriter) WriteByte(c byte) error {
	_, err := m.WriteString(string(c))
	return err
}

// WriteRune writes a single rune.
func (m *MarkdownWriter) WriteRune(r rune) (int, error) {
	return m.WriteString(string(r))
}

// WriteString writes a string.
func (m *MarkdownWriter) WriteString(snippet string) (int, error) {
	if snippet == "" {
		return 0, nil
	}

	// check whether current chunk exceeds chunk size, if so, apply to chunks
	if utf8.RuneCountInString(m.curSnippet)+utf8.RuneCountInString(snippet) >= m.chunkSize {
		m.flush()
		if !m.hTitlePrepended && m.hTitle != "" && !strings.Contains(m.curSnippet, m.hTitle) {
			// prepend `Header Title` to chunk
			m.curSnippet = fmt.Sprintf("%s\n%s", m.hTitle, snippet)
			m.hTitlePrepended = true
		} else {
			m.curSnippet = snippet
		}
	} else {
		m.curSnippet = fmt.Sprintf("%s%s", m.curSnippet, snippet)
	}
	return len(snippet), nil
}

// Write writes bytes.
func (m *MarkdownWriter) Write(p []byte) (int, error) {
	return m.WriteString(string(p))
}

func (m *MarkdownWriter) clone() *MarkdownWriter {
	return &MarkdownWriter{
		hTitle:          m.hTitle,
		hTitlePrepended: m.hTitlePrepended,

		orderedList: m.orderedList,
		bulletList:  m.bulletList,

		indentLevel: m.indentLevel,

		chunkSize:      m.chunkSize,
		chunkOverlap:   m.chunkOverlap,
		secondSplitter: m.secondSplitter,
	}
}

func (m *MarkdownWriter) flush() {
	defer func() {
		m.curSnippet = ""
	}()

	var chunks []string
	if m.curSnippet != "" {
		// check whether current chunk is over ChunkSize，if so, re-split current chunk
		if utf8.RuneCountInString(m.curSnippet) <= m.chunkSize+m.chunkOverlap {
			chunks = []string{m.curSnippet}
		} else {
			// split current snippet to chunks
			chunks, _ = m.secondSplitter.SplitText(m.curSnippet)
		}
	}

	// if there is only H1/H2 and so on, just apply the `Header Title` to chunks
	if len(chunks) == 0 && m.hTitle != "" && !m.hTitlePrepended {
		m.chunks = append(m.chunks, m.hTitle)
		m.hTitlePrepended = true
		return
	}

	for _, chunk := range chunks {
		if chunk == "" {
			continue
		}
		m.chunks = append(m.chunks, chunk)
	}
}
