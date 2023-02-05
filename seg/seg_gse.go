package seg

import (
	"github.com/go-ego/gse"
	"path"
)

type GseTokenizer struct {
	seg *gse.Segmenter
}

func NewGseTokenizer(filePath string) (*GseTokenizer, error) {
	seg := new(gse.Segmenter)
	err := seg.LoadDict(path.Join(filePath, "s_1.txt"))
	if err != nil {
		return nil, err
	}
	return &GseTokenizer{
		seg: seg,
	}, nil
}

func (g *GseTokenizer) Seg(text string) []string {
	return g.seg.CutSearch(text, true)
}
