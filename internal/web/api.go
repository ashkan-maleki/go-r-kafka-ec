package web

import (
	"context"
	"encoding/json"
	"log"

	"github.com/mamalmaleki/go-r-kafka-ec/internal/config"
	"github.com/mamalmaleki/go-r-kafka-ec/internal/domain/contract"
	"github.com/mamalmaleki/go-r-kafka-ec/internal/domain/model"
	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"
	"github.com/teris-io/shortid"
)

type API struct {
	rdb           *redis.Client
	newPostWriter *kafka.Writer
}

func NewAPI() (*API, func()) {
	p := &API{}

	//mechanism, err := scram.Mechanism(scram.SHA256, "","")
	//if err != nil {
	//	log.Fatalln(err)
	//}

	// setup redis
	opt, _ := redis.ParseURL(config.RedisUrl)
	p.rdb = redis.NewClient(opt)

	// setup kafka
	//dialer := &kafka.Dialer{SASLMechanism: mechanism, TLS: &tls.Config{}}
	//dialer := &kafka.Dialer{} // TODO: Fill in the dialer

	// tlsConfig := &tls.Config{
	// 	InsecureSkipVerify: true, // Set this to false in production with valid certificates
	// }

	p.newPostWriter = &kafka.Writer{
		Addr:  kafka.TCP(config.KafkaBrokerAddress),
		Topic: config.KafkaTopicNewPosts,
		// Transport: &kafka.Transport{
		// 	TLS: tlsConfig,
		// },
	}

	return p, func() {
		p.newPostWriter.Close()
		p.rdb.Close()
	}
}

// NewMessage returns the generated UID and error
func (a *API) NewMessage(title, content string) (string, error) {
	log.Println("api new message begins")
	defer func() {
		log.Println("api new message ends")
	}()
	uid := shortid.MustGenerate()
	log.Println(uid)
	message := contract.NewPostMessage{
		UID:     uid,
		Title:   title,
		Content: content,
	}
	b, _ := json.Marshal(message)
	return uid, a.newPostWriter.WriteMessages(context.Background(), kafka.Message{Value: b})
}

func (a *API) GetPost(slug string) (model.Post, error) {
	var p model.Post
	tr := a.rdb.Get(context.Background(), "post:"+slug)
	b, err := tr.Bytes()
	if err != nil {
		return model.Post{}, err
	}
	json.Unmarshal(b, &p)
	return p, nil
}
