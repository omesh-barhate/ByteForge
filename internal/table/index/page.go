package index

type Page struct {
	StartPos int64
	Content  []byte
}

func NewPage(startPos int64) *Page {
	return &Page{
		StartPos: startPos,
	}
}

func NewPageWithContent(startPos int64, content []byte) *Page {
	page := NewPage(startPos)
	page.Content = content
	return page
}
