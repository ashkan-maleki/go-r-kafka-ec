package model

type Post struct {
	UID     string `json:"uid" gorm:"primary"`
	Title   string `json:"title"`
	Content string `json:"content"`
	Slug    string `json:"slug" gorm:"uniqueIndex`
}
