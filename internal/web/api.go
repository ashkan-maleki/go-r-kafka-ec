package web

import (
	"crypto/tls"
	"github.com/mamalmaleki/go-r-kafka-ec/internal/config"
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
	p.newPostWriter = &kafka.Writer{
		Addr:  kafka.TCP([]string{""}...),
		Topic: "app.publishedPosts",
		Transport: &kafka.Transport{
			TLS: &tls.Config{},
		},
	}

	return p, func() {
		p.newPostWriter.Close()
		p.rdb.Close()
	}
}

// NewMessage returns the generated UID and error
func (a *API) NewMessage(title, content string) (string, error) {
	uid := shortid.MustGenerate()
}
