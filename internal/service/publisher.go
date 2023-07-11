package service

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"io"
	"log"
	"time"

	"github.com/Shopify/sarama"
	"github.com/gosimple/slug"
	"github.com/mamalmaleki/go-r-kafka-ec/internal/config"
	"github.com/mamalmaleki/go-r-kafka-ec/internal/domain/contract"
	"github.com/mamalmaleki/go-r-kafka-ec/internal/domain/model"
	"github.com/segmentio/kafka-go"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

type Publisher struct {
	newPostReader       *kafka.Reader
	publishedPostWriter *kafka.Writer

	newPostConsumer       sarama.Consumer
	publishedPostProducer sarama.SyncProducer
	db                    *gorm.DB
}

func NewPublisher() (*Publisher, func()) {

	p := &Publisher{}

	// setup database
	//dsn := "postgres://pg:pass@localhost:5432/crud"
	//dsn := "host=localhost user= password= dbname=posts sslmode=disable TimeZone=Asia/Tehran"
	db, err := gorm.Open(postgres.Open(config.PostgresDsn), &gorm.Config{})

	if err != nil {
		log.Fatalln(err)
	}
	p.db = db
	if err := db.AutoMigrate(&model.Post{}); err != nil {
		log.Fatalln(err)
	}

	// setup kafka

	brokers := []string{config.KafkaBrokerAddress}

	// to create topics when auto.create.topics.enable='false'
	// topic1 := "app.newPosts"
	// topic2 := "app.publishedPosts"

	configSarama := sarama.NewConfig()
	configSarama.Consumer.Return.Errors = true
	configSarama.Consumer.Offsets.Initial = sarama.OffsetOldest
	consumer, err := sarama.NewConsumer(brokers, configSarama)
	if err != nil {
		panic(err.Error())
	}
	p.newPostConsumer = consumer

	configSarama = sarama.NewConfig()
	configSarama.Producer.Return.Successes = true
	producer, err := sarama.NewSyncProducer(brokers, configSarama)
	if err != nil {
		log.Fatalf("Failed to create producer: %v", err)
	}
	p.publishedPostProducer = producer

	return p, func() {
		p.newPostConsumer.Close()
		p.publishedPostProducer.Close()
	}
}

func NewPublisher2() (*Publisher, func()) {

	p := &Publisher{}

	//mechanism, err := scram.Mechanism(scram.SHA256, "", "")
	//if err != nil {
	//	log.Fatalln(err)
	//}

	// setup database
	//dsn := "postgres://pg:pass@localhost:5432/crud"
	//dsn := "host=localhost user= password= dbname=posts sslmode=disable TimeZone=Asia/Tehran"
	db, err := gorm.Open(postgres.Open(config.PostgresDsn), &gorm.Config{})

	if err != nil {
		log.Fatalln(err)
	}
	p.db = db
	if err := db.AutoMigrate(&model.Post{}); err != nil {
		log.Fatalln(err)
	}

	// setup kafka
	//dialer := &kafka.Dialer{SASLMechanism: mechanism, TLS: &tls.Config{}}
	//dialer := &kafka.Dialer{
	//	TLS: &tls.Config{InsecureSkipVerify: true},
	//} // TODO: Fill in the dialer
	brokers := []string{config.KafkaBrokerAddress}

	// to create topics when auto.create.topics.enable='false'
	topic1 := "app.newPosts"
	topic2 := "app.publishedPosts"

	tlsConfig := &tls.Config{
		InsecureSkipVerify: true, // Set this to false in production with valid certificates
	}

	p.newPostReader = kafka.NewReader(kafka.ReaderConfig{
		Brokers: brokers,
		Topic:   topic1,
		GroupID: "service.publisher",
		// Partition: 0,
		// MinBytes:  10e3,
		// MaxBytes:  10e6,
		Dialer: &kafka.Dialer{
			TLS: tlsConfig,
		},
	})
	// fmt.Println("pass reader")
	//kafka.NewWriter()
	// p.publishedPostWriter = &kafka.Writer{
	// 	Addr:  kafka.TCP(brokers...),
	// 	Topic: topic2,
	// 	Transport: &kafka.Transport{
	// 		TLS: &tls.Config{},
	// 	},
	// }
	// p.publishedPostWriter = &kafka.Writer{
	// 	Addr:      kafka.TCP(config.KafkaBrokerAddress),
	// 	Topic:     topic2,
	// 	Balancer:  &kafka.LeastBytes{},
	// 	BatchSize: 10,
	// 	// Dialer: &kafka.Dialer{
	// 	// 	TLS: tlsConfig,
	// 	// },
	// }

	p.publishedPostWriter = kafka.NewWriter(kafka.WriterConfig{
		Brokers: brokers, // Replace with your Kafka broker address
		Topic:   topic2,  // Replace with your Kafka topic
		Dialer: &kafka.Dialer{
			TLS: tlsConfig,
		},
	})

	return p, func() {
		p.newPostReader.Close()
		p.publishedPostWriter.Close()
	}
}

func (p *Publisher) Run() {

	partitionConsumer, err := p.newPostConsumer.ConsumePartition(config.KafkaTopicNewPosts, 0, sarama.OffsetOldest)
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		if err := partitionConsumer.Close(); err != nil {
			log.Fatal(err)
		}
	}()

	for newPost := range partitionConsumer.Messages() {
		//fmt.Printf("Consumed message from topic %s partition %d at offset %d: %s\n", message.Topic, message.Partition, message.Offset, string(message.Value))

		var post contract.NewPostMessage
		if err := json.Unmarshal(newPost.Value, &post); err != nil {
			log.Printf("decoding new post err: %s\n", err.Error())
			continue
		}

		postModel := model.Post{
			UID:     post.UID,
			Title:   post.Title,
			Content: post.Content,
			Slug:    slug.Make(post.Title + "-" + time.Now().Format(time.Stamp)),
		}
		log.Println(postModel)

		if err := p.db.Create(&postModel).Error; err != nil {
			log.Printf("saving new post in database: %s\n", err.Error())
		}

		b, _ := json.Marshal(contract.PublishedPostMessage{Post: postModel})

		msg := &sarama.ProducerMessage{
			Topic: config.KafkaTopicPublishedPosts,
			Value: sarama.ByteEncoder(b),
		}

		// partition, offset, err := a.newPostProducer.SendMessage(msg)
		_, _, err = p.publishedPostProducer.SendMessage(msg)
		log.Printf("the %s post has been saved in the database\n", post.UID)
		sarama.ConsumerGroupSession.MarkMessage(nil, newPost, "")
	}

	for {

	}
}

func (p *Publisher) Run2() {

	for {
		newPost, err := p.newPostReader.FetchMessage(context.Background())
		if err != nil {
			if errors.Is(err, io.EOF) {
				return
			}
			log.Fatalln(err)
		}

		var post contract.NewPostMessage
		if err := json.Unmarshal(newPost.Value, &post); err != nil {
			log.Printf("decoding new post err: %s\n", err.Error())
			continue
		}

		postModel := model.Post{
			UID:     post.UID,
			Title:   post.Title,
			Content: post.Content,
			Slug:    slug.Make(post.Title + "-" + time.Now().Format(time.Stamp)),
		}
		log.Println(postModel.Slug)
		if err := p.db.Create(&postModel).Error; err != nil {
			log.Printf("saving new post in database: %s\n", err.Error())
		}
		p.newPostReader.CommitMessages(context.Background(), newPost)

		b, _ := json.Marshal(contract.PublishedPostMessage{Post: postModel})
		p.publishedPostWriter.WriteMessages(context.Background(), kafka.Message{Value: b})
		log.Printf("the %s post has been saved in the database\n", post.UID)
	}
}
