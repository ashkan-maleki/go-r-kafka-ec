package service

type Publisher struct {
	newPostReader *kafka.Reader
	publishedPostWriter
}
