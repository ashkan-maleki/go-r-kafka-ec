package service

import (
	"github.com/mamalmaleki/go-r-kafka-ec/internal/domain/model"
	"github.com/segmentio/kafka-go"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"log"
)

type Publisher struct {
	newPostReader       *kafka.Reader
	publishedPostWriter *kafka.Writer
	db                  *gorm.DB
}

func NewPublisher() (*Publisher, func()) {

	p := &Publisher{}

	//mechanism, err := scram.Mechanism(scram.SHA256, "","")
	//if err != nil {
	//	log.Fatalln(err)
	//}

	// setup database
	dsn := "postgres://pg:pass@localhost:5432/crud"
	//dsn := "host=localhost user= password= dbname=posts sslmode=disable TimeZone=Asia/Tehran"
	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})

	if err != nil {
		log.Fatalln(err)
	}
	p.db = db
	if err := db.AutoMigrate(&model.Post{}); err != nil {
		log.Fatalln(err)
	}
	return p, func() {
		p.newPostReader.Close()
		p.publishedPostWriter.Close()
	}
}
