package fulltext

import (
	"github.com/744189447/fulltext/seg"
	"log"
	"testing"
)

func TestFulltextEn(t *testing.T) {
	index := "en"

	docs := make(map[string]string)

	docs["document_0"] = "a a a b c d h"
	docs["document_1"] = "a b b c"
	docs["document_2"] = "c d d h"
	docs["document_3"] = "a c d h l"
	docs["document_4"] = "d"

	tokenizer := &seg.EnTokenizer{}

	fulltext, err := New("./en", tokenizer)
	if err != nil {
		log.Fatal(err)
	}
	defer fulltext.Free()

	err = fulltext.AddDocs(index, docs)
	if err != nil {
		log.Fatal(err)
	}

	terms, err := fulltext.Suggest(index, "a")
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("len: %d", len(terms))
	for _, term := range terms {
		log.Printf("t: %s", term)
	}

	query := new(Query)
	query.Index(index).Should("a").Must("c", "d").MustNot("l", "k").Limit(0, 5)
	hits, err := fulltext.Search(query)
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("total: %d took: %d", hits.Total, hits.Took)
	for _, doc := range hits.Docs {
		log.Printf("id: %s -- score: %f", doc.ID, doc.Score)
	}

	err = fulltext.RemoveDocs(index, "document_0", "document_1", "document_2", "document_3", "document_4", "document_6")
	if err != nil {
		log.Fatal(err)
	}

	err = fulltext.RemoveIndex(index)
	if err != nil {
		log.Fatal(err)
	}

	err = fulltext.RemoveDB()
	if err != nil {
		log.Fatal(err)
	}
}

func TestFulltextCh(t *testing.T) {
	index := "ch"

	docs := make(map[string]string)

	docs["document_0"] = "在信息检索中，Okapi BM25（BM是最佳匹配的缩写）是搜索引擎用来估计文档与给定搜索查询的相关性的排名函数。它基于Stephen E. Robertson、Karen Spärck Jones等人 在 1970 年代和 1980 年代开发的概率检索框架。"
	docs["document_1"] = "实际排名函数的名称是BM25。更完整的名称Okapi BM25包括第一个使用它的系统的名称，即 80 年代和 90 年代在伦敦城市大学实施的Okapi 信息检索系统。BM25 及其较新的变体，例如 BM25F（BM25 的一个版本，可以考虑文档结构和锚文本），代表用于文档检索的类似TF-IDF的检索功能。"
	docs["document_2"] = "BM25 是一种词袋检索功能，它根据每个文档中出现的查询词对一组文档进行排名，而不管它们在文档中的接近程度。它是一系列评分函数，组件和参数略有不同。该函数最突出的实例之一如下。"
	docs["document_3"] = "其中 N是集合中文档的总数，并且{\\displaystyle n(q_{i})}n(q_i)是包含文件的数量{\\displaystyle q_{i}}q_{i}."
	docs["document_4"] = "IDF 有多种解释，其公式略有不同。在最初的 BM25 推导中，IDF 组件是从Binary Independence Model中推导出来的。"

	tokenizer, err := seg.NewGseTokenizer("./data")
	if err != nil {
		log.Fatal(err)
	}

	fulltext, err := New("./ch", tokenizer)
	if err != nil {
		log.Fatal(err)
	}
	defer fulltext.Free()

	err = fulltext.AddDocs(index, docs)
	if err != nil {
		log.Fatal(err)
	}

	query := &Query{}
	query.Index(index).Match("BM25 是一种词袋检索功能")
	hits, err := fulltext.Search(query)
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("total: %d took: %d", hits.Total, hits.Took)
	for _, doc := range hits.Docs {
		log.Printf("id: %s -- score: %f", doc.ID, doc.Score)
	}

	err = fulltext.RemoveDB()
	if err != nil {
		log.Fatal(err)
	}
}
