package service

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log"
	"time"

	"github.com/Shopify/sarama"
	"github.com/mamalmaleki/go-r-kafka-ec/internal/config"
	"github.com/mamalmaleki/go-r-kafka-ec/internal/domain/contract"
	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"
)

type CacheManager struct {
	publishedPostReader *kafka.Reader

	publishedPostConsumer sarama.Consumer
	rdb                   *redis.Client
}

func NewCacheManager() (*CacheManager, func()) {
	cm := &CacheManager{}

	// setup redis
	opt, _ := redis.ParseURL(config.RedisUrl)
	cm.rdb = redis.NewClient(opt)

	// setup kafka
	configSarama := sarama.NewConfig()
	configSarama.Consumer.Return.Errors = true
	configSarama.Consumer.Offsets.AutoCommit.Enable = true
	configSarama.Consumer.Offsets.AutoCommit.Interval = 1 * time.Second
	consumer, err := sarama.NewConsumer([]string{config.KafkaBrokerAddress}, configSarama)
	if err != nil {
		panic(err.Error())
	}
	cm.publishedPostConsumer = consumer

	return cm, func() {
		cm.publishedPostConsumer.Close()
		cm.rdb.Close()
	}
}

func NewCacheManager2() (*CacheManager, func()) {
	cm := &CacheManager{}
	//mechanism, err := scram.Mechanism(scram.SHA256, "", "")
	//if err != nil {
	//	log.Fatalln(err)
	//}

	// setup redis
	opt, _ := redis.ParseURL(config.RedisUrl)
	cm.rdb = redis.NewClient(opt)

	// setup kafka
	//dialer := &kafka.Dialer{SASLMechanism: mechanism, TLS: &tls.Config{}}
	//dialer := &kafka.Dialer{
	//	//Timeout:   10 * time.Second,
	//	//DualStack: true,
	//	TLS: &tls.Config{InsecureSkipVerify: true},
	//} // TODO: Fill in the dialer
	cm.publishedPostReader = kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{config.KafkaBrokerAddress},
		Topic:   "app.publishedPosts",
		GroupID: "service.cacheManager",
		//Dialer:  dialer,
	})

	return cm, func() {
		cm.publishedPostReader.Close()
		cm.rdb.Close()
	}
}

func (c *CacheManager) Run() {

	partitionConsumer, err := c.publishedPostConsumer.ConsumePartition(config.KafkaTopicPublishedPosts, 0, sarama.OffsetOldest)
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		if err := partitionConsumer.Close(); err != nil {
			log.Fatal(err)
		}
	}()

	for publishedPost := range partitionConsumer.Messages() {
		var published contract.PublishedPostMessage
		if err := json.Unmarshal(publishedPost.Value, &published); err != nil {
			log.Printf("decoding published post error: %s\n", err.Error())
			continue
		}
		log.Println(published.Post)
		b, _ := json.Marshal(published.Post)
		c.rdb.Set(context.Background(), "post:"+published.Slug, b, 0)
		log.Printf("the %s post has been saved in Redis\n", published.UID)
	}

}

func (c *CacheManager) Run2() {
	for {
		publishedPost, err := c.publishedPostReader.FetchMessage(context.Background())
		if err != nil {
			if errors.Is(err, io.EOF) {
				return
			}
			log.Fatalln(err)
		}

		var published contract.PublishedPostMessage
		if err := json.Unmarshal(publishedPost.Value, &published); err != nil {
			log.Printf("decoding published post error: %s\n", err.Error())
			continue
		}

		b, _ := json.Marshal(published.Post)
		c.rdb.Set(context.Background(), "post:"+published.Slug, b, 0)
		c.publishedPostReader.CommitMessages(context.Background(), publishedPost)
		log.Printf("the %s post has been saved in Redis\n", published.UID)
	}
}
