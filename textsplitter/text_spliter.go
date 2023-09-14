package textsplitter

// Chunk associates metadata with a chunk of text.
type Chunk struct {
	Text     string
	Metadata map[string]any
}

// TextSplitter is the standard interface for splitting texts.
type TextSplitter interface {
	SplitText(string) ([]Chunk, error)
}
