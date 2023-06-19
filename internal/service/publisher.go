package service

import (
	"gorm.io/gorm"
)

type Publisher struct {
	newPostReader       *kafka.Reader
	publishedPostWriter *kafka.Writer
	db                  *gorm.DB
}
