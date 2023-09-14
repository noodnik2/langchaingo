package textsplitter

import (
	"fmt"
	"strings"
	"unicode/utf8"

	"github.com/yuin/goldmark"
	"github.com/yuin/goldmark/ast"
	"github.com/yuin/goldmark/extension"
	extensionAst "github.com/yuin/goldmark/extension/ast"
	"github.com/yuin/goldmark/text"
	"github.com/yuin/goldmark/util"
)

// NewMarkdownTextSplitterV2 creates a new MarkdownTextSplitterV2.
func NewMarkdownTextSplitterV2(opts ...Option) *MarkdownTextSplitterV2 {
	options := DefaultOptions()
	for _, o := range opts {
		o(&options)
	}

	sp := &MarkdownTextSplitterV2{
		ChunkSize:      options.ChunkSize,
		ChunkOverlap:   options.ChunkOverlap,
		SecondSplitter: options.SecondSplitter,
		LevelHeaderFn:  options.LevelHeaderFn,
	}

	sp.nodeRenderers = map[ast.NodeKind]NodeRender{
		ast.KindDocument:        sp.renderDocument,
		ast.KindHeading:         sp.renderHeading,
		ast.KindParagraph:       sp.renderParagraph,
		ast.KindList:            sp.renderList,
		ast.KindListItem:        sp.renderListItem,
		ast.KindEmphasis:        sp.renderEmphasis,
		ast.KindTextBlock:       sp.renderTextBlock,
		ast.KindAutoLink:        sp.renderAutoLink,
		ast.KindBlockquote:      sp.renderBlockQuote,
		ast.KindFencedCodeBlock: sp.renderFencedCodeBlock,

		// table
		extensionAst.KindTable:       sp.renderTable,
		extensionAst.KindTableHeader: sp.renderTableHeader,
		extensionAst.KindTableRow:    sp.renderTableRow,
		extensionAst.KindTableCell:   sp.renderTableCell,

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

// MarkdownTextSplitterV2 Markdown text splitter.
type MarkdownTextSplitterV2 struct {
	ChunkSize    int
	ChunkOverlap int
	// SecondSplitter splits paragraphs
	SecondSplitter TextSplitter

	// nodeRenderers is a map of node kind and NodeRender.
	nodeRenderers map[ast.NodeKind]NodeRender
	LevelHeaderFn LevelHeaderFn
}

// SplitText splits a text into multiple text.
func (m *MarkdownTextSplitterV2) SplitText(s string) ([]Chunk, error) {
	reader := text.NewReader([]byte(s))

	gm := goldmark.New(
		goldmark.WithExtensions(
			extension.NewTable(
				extension.WithTableCellAlignMethod(extension.TableCellAlignDefault),
			),
		),
	)

	node := gm.Parser().Parse(reader)
	node.Dump(reader.Source(), 0)

	writer := &MarkdownWriter{
		chunkSize:      m.ChunkSize,
		chunkOverlap:   m.ChunkOverlap,
		secondSplitter: m.SecondSplitter,
		levelHeaderFn:  m.LevelHeaderFn,
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
type NodeRender = func(w *MarkdownWriter, source []byte, node ast.Node, entering bool) (ast.WalkStatus, error)

// Render renders a markdown node.
func (m *MarkdownTextSplitterV2) Render(writer *MarkdownWriter, n ast.Node, source []byte) error {
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
	*MarkdownWriter, []byte, ast.Node, bool,
) (ast.WalkStatus, error) {
	return ast.WalkContinue, nil
}

// renderHeading renders a heading node.
func (m *MarkdownTextSplitterV2) renderHeading(
	w *MarkdownWriter, source []byte, node ast.Node, entering bool,
) (ast.WalkStatus, error) {
	n, _ := node.(*ast.Heading)
	if !entering {
		w.joinSnippet("\n")
		return ast.WalkContinue, nil
	}

	w.applyToChunks()

	hTitle := string(n.Text(source))
	w.joinSnippet(fmt.Sprintf("%s %s", strings.Repeat("#", n.Level), hTitle))

	w.hTitle = hTitle // TODO (noodnik2): Check that it fails a test if this isn't done here
	w.hTitlePrepended = false

	if n.Level != len(w.headers) {
		newHeaders := make([]string, n.Level)
		copy(newHeaders, w.headers)
		w.headers = newHeaders
	}
	w.headers[n.Level-1] = hTitle

	return ast.WalkSkipChildren, nil
}

// renderParagraph renders a paragraph node.
func (m *MarkdownTextSplitterV2) renderParagraph(
	w *MarkdownWriter, _ []byte, _ ast.Node, entering bool,
) (ast.WalkStatus, error) {
	if !entering {
		w.joinSnippet("\n")
		return ast.WalkContinue, nil
	}

	return ast.WalkContinue, nil
}

// renderList renders a list node.
func (m *MarkdownTextSplitterV2) renderList(
	w *MarkdownWriter, source []byte, node ast.Node, entering bool,
) (ast.WalkStatus, error) {
	n, _ := node.(*ast.List)

	w.joinSnippet("\n")

	if !entering {
		return ast.WalkContinue, nil
	}

	nnw := w.clone()
	if n.IsOrdered() {
		nnw.orderedList = true
	} else {
		nnw.bulletList = true
	}

	for c := n.FirstChild(); c != nil; c = c.NextSibling() {
		nnw.indentLevel = w.indentLevel
		nnw.indentLevel++

		err := m.Render(nnw, c, source)
		if err != nil {
			return ast.WalkStop, err
		}

		nnw.applyToChunks()

		for i := range nnw.chunks {
			var txt string
			if nnw.indentLevel > 1 {
				txt = formatWithIndent(nnw.chunks[i].Text, "  ")
			} else {
				txt = nnw.chunks[i].Text
			}
			if i < (len(nnw.chunks) - 1) {
				txt += "\n"
			}
			w.joinSnippet(txt)
		}

		nnw.chunks = []Chunk{}
	}

	return ast.WalkSkipChildren, nil
}

// renderListItem renders a list item node.
func (m *MarkdownTextSplitterV2) renderListItem(
	w *MarkdownWriter, _ []byte, node ast.Node, entering bool,
) (ast.WalkStatus, error) {
	if !entering {
		w.joinSnippet("\n")
		return ast.WalkContinue, nil
	}

	n, _ := node.(*ast.ListItem)
	if n.Parent().Kind() == ast.KindList {
		w.listOrder++
	}
	if w.orderedList {
		w.joinSnippet(fmt.Sprintf("%d. ", w.listOrder))
	} else {
		w.joinSnippet("- ")
	}

	return ast.WalkContinue, nil
}

// renderTable renders a table node.
func (m *MarkdownTextSplitterV2) renderTable(
	w *MarkdownWriter, _ []byte, _ ast.Node, entering bool,
) (ast.WalkStatus, error) {
	if entering {
		w.applyToChunks()
	} else {
		m.splitTableHeaderFirst(w, w.curTHeaders, w.curTRows)
	}
	return ast.WalkContinue, nil
}

// splitTableHeaderFirst splits table header first.
func (m *MarkdownTextSplitterV2) splitTableHeaderFirst(w *MarkdownWriter, header []string, rows [][]string) {
	defer func() {
		w.curTHeaders = []string{}
		w.curTRows = [][]string{}
	}()

	headnoteEmpty := false
	for _, h := range header {
		if h != "" {
			headnoteEmpty = true
			break
		}
	}

	// Sometime, there is no header in table, put the real table header to the first row
	if !headnoteEmpty && len(rows) != 0 {
		header = rows[0]
		rows = rows[1:]
	}

	headerMD := tableHeaderInMarkdown(header)
	if len(rows) == 0 {
		w.chunks = append(w.chunks, Chunk{Text: headerMD})
		return
	}
	// append table header
	for _, row := range rows {
		line := tableRowInMarkdown(row)
		w.chunks = append(w.chunks, Chunk{Text: fmt.Sprintf("%s\n%s", headerMD, line)})
	}
}

// renderTableHeader renders a table header node.
func (m *MarkdownTextSplitterV2) renderTableHeader(
	w *MarkdownWriter, _ []byte, _ ast.Node, entering bool,
) (ast.WalkStatus, error) {
	if !entering {
		w.curTHeaders = w.curRow
		w.curRow = []string{}
	}
	return ast.WalkContinue, nil
}

// renderTableRow renders a table row node.
func (m *MarkdownTextSplitterV2) renderTableRow(
	w *MarkdownWriter, _ []byte, _ ast.Node, entering bool,
) (ast.WalkStatus, error) {
	if !entering {
		if len(w.curTHeaders) == 0 {
			w.curTHeaders = w.curRow
		} else {
			w.curTRows = append(w.curTRows, w.curRow)
		}
		w.curRow = []string{}
	}
	return ast.WalkContinue, nil
}

// renderTableCell renders a table cell node.
func (m *MarkdownTextSplitterV2) renderTableCell(
	w *MarkdownWriter, source []byte, n ast.Node, entering bool,
) (ast.WalkStatus, error) {
	if entering {
		source := n.Text(source)
		w.curRow = append(w.curRow, string(source))
	}
	return ast.WalkContinue, nil
}

// renderEmphasis renders an emphasis node.
func (m *MarkdownTextSplitterV2) renderEmphasis(
	w *MarkdownWriter, _ []byte, _ ast.Node, entering bool,
) (ast.WalkStatus, error) {
	if !entering {
		w.joinSnippet("**")
		return ast.WalkContinue, nil
	}

	w.joinSnippet("**")
	return ast.WalkContinue, nil
}

// renderTextBlock renders a text block node.
func (m *MarkdownTextSplitterV2) renderTextBlock(
	w *MarkdownWriter, _ []byte, node ast.Node, entering bool,
) (ast.WalkStatus, error) {
	if !entering {
		if node.NextSibling() != nil && node.FirstChild() != nil {
			w.joinSnippet("\n")
		}
	}

	return ast.WalkContinue, nil
}

// renderBlockQuote renders a block quote node.
func (m *MarkdownTextSplitterV2) renderBlockQuote(
	w *MarkdownWriter, source []byte, node ast.Node, entering bool,
) (ast.WalkStatus, error) {
	n, _ := node.(*ast.Blockquote)
	if !entering {
		return ast.WalkContinue, nil
	}

	w.joinSnippet(fmt.Sprintf("```%s\n```", n.Text(source)))

	return ast.WalkContinue, nil
}

func (m *MarkdownTextSplitterV2) renderFencedCodeBlock(
	w *MarkdownWriter, source []byte, node ast.Node, entering bool,
) (ast.WalkStatus, error) {
	n, _ := node.(*ast.FencedCodeBlock)
	w.joinSnippet("```\n")
	if entering {
		m.joinNodeLines(w, source, n)
	}
	return ast.WalkContinue, nil
}

func (m *MarkdownTextSplitterV2) joinNodeLines(w *MarkdownWriter, source []byte, n ast.Node) {
	l := n.Lines().Len()
	for i := 0; i < l; i++ {
		line := n.Lines().At(i)
		m.joinEscaped(w, line.Value(source))
	}
}

func (m *MarkdownTextSplitterV2) joinEscaped(w *MarkdownWriter, source []byte) {
	n := 0
	l := len(source)
	for i := 0; i < l; i++ {
		v := util.EscapeHTMLByte(source[i])
		if v != nil {
			w.joinSnippet(string(source[i-n : i]))
			n = 0
			w.joinSnippet(string(v))
			continue
		}
		n++
	}
	if n != 0 {
		w.joinSnippet(string(source[l-n:]))
	}
}

// renderAutoLink renders an auto link node.
func (m *MarkdownTextSplitterV2) renderAutoLink(
	w *MarkdownWriter, source []byte, node ast.Node, entering bool,
) (ast.WalkStatus, error) {
	n, _ := node.(*ast.AutoLink)
	if !entering {
		return ast.WalkContinue, nil
	}

	w.joinSnippet(fmt.Sprintf("[%s](%s)", n.Label(source), n.URL(source)))

	return ast.WalkContinue, nil
}

// renderLink renders a link node.
func (m *MarkdownTextSplitterV2) renderLink(
	w *MarkdownWriter, source []byte, node ast.Node, entering bool,
) (ast.WalkStatus, error) {
	n, _ := node.(*ast.Link)
	if !entering {
		return ast.WalkContinue, nil
	}

	w.joinSnippet(fmt.Sprintf("[%s](%s)", n.Text(source), n.Destination))

	return ast.WalkSkipChildren, nil
}

// renderText renders a text node.
func (m *MarkdownTextSplitterV2) renderText(
	w *MarkdownWriter, source []byte, node ast.Node, entering bool,
) (ast.WalkStatus, error) {
	if !entering {
		return ast.WalkContinue, nil
	}

	n, _ := node.(*ast.Text)
	txt := string(n.Text(source))
	if n.SoftLineBreak() {
		txt += "\n"
	}

	w.joinSnippet(txt)

	return ast.WalkContinue, nil
}

// renderString renders a string node.
func (m *MarkdownTextSplitterV2) renderString(
	w *MarkdownWriter, _ []byte, node ast.Node, entering bool,
) (ast.WalkStatus, error) {
	if !entering {
		return ast.WalkContinue, nil
	}

	n, _ := node.(*ast.String)
	w.joinSnippet(string(n.Value))
	return ast.WalkContinue, nil
}

// =================================================================================================================//
// =================================================================================================================//
// =================================================================================================================//
// =================================================================================================================//
// =================================================================================================================//
// =================================================================================================================//

// MarkdownWriter writes Markdown text to chunks.
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

	// curTHeaders current table headers
	curTHeaders []string
	// curRow current table row
	curRow []string
	// curTRows current table rows
	curTRows [][]string

	curSnippet   string
	chunkSize    int
	chunkOverlap int
	chunks       []Chunk
	// headers represents the currently active set of headers
	headers []string
	// secondSplitter re-split markdown single long paragraph into chunks
	secondSplitter TextSplitter
	levelHeaderFn  LevelHeaderFn
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
	m.applyToChunks()
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
	m.joinSnippet(snippet)
	return len(snippet), nil
}

// joinSnippet join sub snippet to current total snippet.
func (m *MarkdownWriter) joinSnippet(snippet string) {
	if snippet == "" {
		return
	}

	// check whether current chunk exceeds chunk size, if so, apply to chunks
	if utf8.RuneCountInString(m.curSnippet)+utf8.RuneCountInString(snippet) >= m.chunkSize {
		m.applyToChunks()
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
		headers:        m.headers,
		levelHeaderFn:  m.levelHeaderFn,
	}
}

func (m *MarkdownWriter) applyToChunks() {
	defer func() {
		m.curSnippet = ""
	}()

	headerMetadata := m.getHeaderMetadata()

	var chunks []Chunk
	if m.curSnippet != "" {
		// check whether current chunk is over ChunkSize，if so, re-split current chunk.
		if utf8.RuneCountInString(m.curSnippet) <= m.chunkSize+m.chunkOverlap {
			chunks = []Chunk{{Text: m.curSnippet, Metadata: headerMetadata}}
		} else {
			// split current snippet into chunks.
			chunks, _ = m.secondSplitter.SplitText(m.curSnippet)
		}
	}

	// if there is only H1/H2 and so on, just apply the `Header Title` to chunks.
	if len(chunks) == 0 && m.hTitle != "" && !m.hTitlePrepended {
		titleChunk := Chunk{Text: m.hTitle, Metadata: headerMetadata}
		m.chunks = append(m.chunks, titleChunk)
		m.hTitlePrepended = true
		return
	}

	for _, chunk := range chunks {
		if chunk.Text == "" {
			continue
		}
		// TODO (noodnik2): is this needed?  Compare to V1 splitter
		//if m.hTitle != "" && !strings.Contains(m.curSnippet, m.hTitle) {
		//	// prepend `Header Title` to chunk
		//	chunk.Text = fmt.Sprintf("%s\n%s", m.hTitle, chunk.Text)
		//}
		m.chunks = append(m.chunks, chunk)
	}
}

// getHeaderMetadata returns metadata related to the current set of headers.
func (m *MarkdownWriter) getHeaderMetadata() map[string]any {
	if m.levelHeaderFn == nil {
		return map[string]any{}
	}
	chunkMetadata := make(map[string]any, len(m.headers))
	for i, hText := range m.headers {
		fn := m.levelHeaderFn(i+1, hText)
		for k, v := range fn {
			chunkMetadata[k] = v
		}
	}
	return chunkMetadata
}
