package service

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/mamalmaleki/go-r-kafka-ec/internal/domain/contract"
	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"
	"io"
	"log"
)

type CacheManager struct {
	publishedPostReader *kafka.Reader
	rdb                 *redis.Client
}

func NewCacheManager() (*CacheManager, func()) {
	cm := &CacheManager{}
	//mechanism, err := scram.Mechanism(scram.SHA256, "", "")
	//if err != nil {
	//	log.Fatalln(err)
	//}

	// setup redis
	opt, _ := redis.ParseURL("redis://default:PASSWORD@SERVER:PORT")
	cm.rdb = redis.NewClient(opt)

	// setup kafka
	// dialer := &kafka.Dialer{SASLMechanism: mechanism, TLS: &tls.Config{}}
	dialer := &kafka.Dialer{} // TODO: Fill in the dialer
	cm.publishedPostReader = kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{""},
		Topic:   "app.publishedPosts",
		GroupID: "service.cacheManager",
		Dialer:  dialer,
	})

	return cm, func() {
		cm.publishedPostReader.Close()
		cm.rdb.Close()
	}
}

func (c *CacheManager) Run() {
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
