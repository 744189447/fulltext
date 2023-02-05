package fulltext

type Tokenizer interface {
	Seg(text string) []string
}
