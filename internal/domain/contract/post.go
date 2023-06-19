package contract

import "github.com/mamalmaleki/go-r-kafka-ec/internal/domain/model"

type NewPostMessage struct {
	UID     string `json:"uid"`
	Title   string `json:"title"`
	Content string `json:"content"`
}

type PublishedPostMessage struct {
	model.Post
}
