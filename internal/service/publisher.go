package service

import (
	kafka "github.com/segmentio/kafka-go"
	"gorm.io/gorm"
)

type Publisher struct {
	newPostReader       *kafka.Reader
	publishedPostWriter *kafka.Writer
	db                  *gorm.DB
}
