package service

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"io"
	"log"
	"time"

	"github.com/gosimple/slug"
	"github.com/mamalmaleki/go-r-kafka-ec/internal/config"
	"github.com/mamalmaleki/go-r-kafka-ec/internal/domain/contract"
	"github.com/mamalmaleki/go-r-kafka-ec/internal/domain/model"
	"github.com/segmentio/kafka-go"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

type PublisherKafkaGo struct {
	newPostReader       *kafka.Reader
	publishedPostWriter *kafka.Writer
	db                  *gorm.DB
}

func NewPublisherKafkaGo() (*PublisherKafkaGo, func()) {

	p := &PublisherKafkaGo{}

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

	// conn, err := kafka.Dial("tcp", config.KafkaBrokerAddress)
	// if err != nil {
	// 	log.Printf("hi")
	// 	panic(err.Error())
	// }
	// defer conn.Close()

	// controller, err := conn.Controller()
	// if err != nil {
	// 	panic(err.Error())
	// }
	// var controllerConn *kafka.Conn
	// controllerConn, err = kafka.Dial("tcp", net.JoinHostPort(controller.Host,
	// 	strconv.Itoa(controller.Port)))
	// if err != nil {
	// 	panic(err.Error())
	// }
	// defer controllerConn.Close()

	// topicConfigs := []kafka.TopicConfig{
	// 	{
	// 		Topic:             topic1,
	// 		NumPartitions:     1,
	// 		ReplicationFactor: 1,
	// 	}, {
	// 		Topic:             topic2,
	// 		NumPartitions:     1,
	// 		ReplicationFactor: 1,
	// 	},
	// }

	// err = controllerConn.CreateTopics(topicConfigs...)
	// if err != nil {
	// 	panic(err.Error())
	// }

	// p.newPostReader = kafka.NewReader(kafka.ReaderConfig{
	// 	Brokers: brokers,
	// 	Topic:   topic1,
	// 	GroupID: "service.publisher",
	// 	//Dialer:  dialer,
	// })
	// Create a Kafka TLS configuration

	const (
		certPEM = `-----BEGIN CERTIFICATE-----
MIID2zCCAsOgAwIBAgIJAMSqbewCgw4xMA0GCSqGSIb3DQEBCwUAMGAxCzAJBgNV
BAYTAlVTMRMwEQYDVQQIDApDYWxpZm9ybmlhMRYwFAYDVQQHDA1TYW4gRnJhbmNp
c2NvMRAwDgYDVQQKDAdTZWdtZW50MRIwEAYDVQQDDAlsb2NhbGhvc3QwHhcNMTcx
MjIzMTU1NzAxWhcNMjcxMjIxMTU1NzAxWjBgMQswCQYDVQQGEwJVUzETMBEGA1UE
CAwKQ2FsaWZvcm5pYTEWMBQGA1UEBwwNU2FuIEZyYW5jaXNjbzEQMA4GA1UECgwH
U2VnbWVudDESMBAGA1UEAwwJbG9jYWxob3N0MIIBIjANBgkqhkiG9w0BAQEFAAOC
AQ8AMIIBCgKCAQEAtda9OWKYNtINe/BKAoB+/zLg2qbaTeHN7L722Ug7YoY6zMVB
aQEHrUmshw/TOrT7GLN/6e6rFN74UuNg72C1tsflZvxqkGdrup3I3jxMh2ApAxLi
zem/M6Eke2OAqt+SzRPqc5GXH/nrWVd3wqg48DZOAR0jVTY2e0fWy+Er/cPJI1lc
L6ZMIRJikHTXkaiFj2Jct1iWvgizx5HZJBxXJn2Awix5nvc+zmXM0ZhoedbJRoBC
dGkRXd3xv2F4lqgVHtP3Ydjc/wYoPiGudSAkhyl9tnkHjvIjA/LeRNshWHbCIaQX
yemnXIcyyf+W+7EK0gXio7uiP+QSoM5v/oeVMQIDAQABo4GXMIGUMHoGA1UdIwRz
MHGhZKRiMGAxCzAJBgNVBAYTAlVTMRMwEQYDVQQIDApDYWxpZm9ybmlhMRYwFAYD
VQQHDA1TYW4gRnJhbmNpc2NvMRAwDgYDVQQKDAdTZWdtZW50MRIwEAYDVQQDDAls
b2NhbGhvc3SCCQCBYUuEuypDMTAJBgNVHRMEAjAAMAsGA1UdDwQEAwIE8DANBgkq
hkiG9w0BAQsFAAOCAQEATk6IlVsXtNp4C1yeegaM+jE8qgKJfNm1sV27zKx8HPiO
F7LvTGYIG7zd+bf3pDSwRxfBhsLEwmN9TUN1d6Aa9zeu95qOnR76POfHILgttu2w
IzegO8I7BycnLjU9o/l9gCpusnN95tIYQhfD08ygUpYTQRuI0cmZ/Dp3xb0S9f5N
miYTuUoStYSA4RWbDWo+Is9YWPu7rwieziOZ96oguGz3mtqvkjxVAQH1xZr3bKHr
HU9LpQh0i6oTK0UCqnDwlhJl1c7A3UooxFpc3NGxyjogzTfI/gnBKfPo7eeswwsV
77rjIkhBW49L35KOo1uyblgK1vTT7VPtzJnuDq3ORg==
-----END CERTIFICATE-----`

		keyPEM = `-----BEGIN RSA PRIVATE KEY-----
MIIEowIBAAKCAQEAtda9OWKYNtINe/BKAoB+/zLg2qbaTeHN7L722Ug7YoY6zMVB
aQEHrUmshw/TOrT7GLN/6e6rFN74UuNg72C1tsflZvxqkGdrup3I3jxMh2ApAxLi
zem/M6Eke2OAqt+SzRPqc5GXH/nrWVd3wqg48DZOAR0jVTY2e0fWy+Er/cPJI1lc
L6ZMIRJikHTXkaiFj2Jct1iWvgizx5HZJBxXJn2Awix5nvc+zmXM0ZhoedbJRoBC
dGkRXd3xv2F4lqgVHtP3Ydjc/wYoPiGudSAkhyl9tnkHjvIjA/LeRNshWHbCIaQX
yemnXIcyyf+W+7EK0gXio7uiP+QSoM5v/oeVMQIDAQABAoIBAQCa6roHW8JGYipu
vsau3v5TOOtsHN67n3arDf6MGwfM5oLN1ffmF6SMs8myv36781hBMRv3FwjWHSf+
pgz9o6zsbd05Ii8/m3yiXq609zZT107ZeYuU1mG5AL5uCNWjvhn5cdA6aX0RFwC0
+tnjEyJ/NCS8ujBR9n/wA8IxrEKoTGcxRb6qFPPKWYoBevu34td1Szf0kH8AKjtQ
rdPK0Of/ZEiAUxNMLTBEOmC0ZabxJV/YGWcUU4DpmEDZSgQSr4yLT4BFUwF2VC8t
8VXn5dBP3RMo4h7JlteulcKYsMQZXD6KvUwY2LaEpFM/b14r+TZTUQGhwS+Ha11m
xa4eNwFhAoGBANshGlpR9cUUq8vNex0Wb63P9BTRTXwg1yEJVMSua+DlaaqaX/hS
hOxl3K4y2V5OCK31C+SOAqqbrGtMXVym5c5pX8YyC11HupFJwdFLUEc74uF3CtWY
GMMvEvItCK5ZvYvS5I2CQGcp1fhEMle/Uz+hFi1eeWepMqgHbVx5vkdtAoGBANRv
XYQsTAGSkhcHB++/ASDskAew5EoHfwtJzSX0BZC6DCACF/U4dCKzBVndOrELOPXs
2CZXCG4ptWzNgt6YTlMX9U7nLei5pPjoivIJsMudnc22DrDS7C94rCk++M3JeLOM
KSN0ou9+1iEdE7rQdMgZMryaY71OBonCIDsWgJZVAoGAB+k0CFq5IrpSUXNDpJMw
yPee+jlsMLUGzzyFAOzDHEVsASq9mDtybQ5oXymay1rJ2W3lVgUCd6JTITSKklO8
LC2FtaQM4Ps78w7Unne3mDrDQByKGZf6HOHQL0oM7C51N10Pv0Qaix7piKL9pklT
+hIYuN6WR3XGTGaoPhRvGCkCgYBqaQ5y8q1v7Dd5iXAUS50JHPZYo+b2niKpSOKW
LFHNWSRRtDrD/u9Nolb/2K1ZmcGCjo0HR3lVlVbnlVoEnk49mTaru2lntfZJKFLR
QsFofR9at+NL95uPe+bhEkYW7uCjL4Y72GT1ipdAJwyG+3xD7ztW9g8X+EmWH8N9
VZw7sQKBgGxp820jbjWhG1O9RnYLwflcZzUlSkhWJDg9tKJXBjD+hFX98Okuf0gu
DUpdbxbJHSi0xAjOjLVswNws4pVwzgtZVK8R7k8j3Z5TtYTJTSQLfgVowuyEdAaI
C8OxVJ/At/IJGnWSIz8z+/YCUf7p4jd2LJgmZVVzXeDsOFcH62gu
-----END RSA PRIVATE KEY-----`

		caPEM = `-----BEGIN CERTIFICATE-----
MIIDPDCCAiQCCQCBYUuEuypDMTANBgkqhkiG9w0BAQsFADBgMQswCQYDVQQGEwJV
UzETMBEGA1UECAwKQ2FsaWZvcm5pYTEWMBQGA1UEBwwNU2FuIEZyYW5jaXNjbzEQ
MA4GA1UECgwHU2VnbWVudDESMBAGA1UEAwwJbG9jYWxob3N0MB4XDTE3MTIyMzE1
NTMxOVoXDTI3MTIyMTE1NTMxOVowYDELMAkGA1UEBhMCVVMxEzARBgNVBAgMCkNh
bGlmb3JuaWExFjAUBgNVBAcMDVNhbiBGcmFuY2lzY28xEDAOBgNVBAoMB1NlZ21l
bnQxEjAQBgNVBAMMCWxvY2FsaG9zdDCCASIwDQYJKoZIhvcNAQEBBQADggEPADCC
AQoCggEBAJwB+Yp6MyUepgtaRDxVjpMI2RmlAaV1qApMWu60LWGKJs4KWoIoLl6p
oSEqnWrpMmb38pyGP99X1+t3uZjiK9L8nFhuKZ581tsTKLxaSl+YVg7JbH5LVCS6
opsfB5ON1gJxf1HA9YyMqKHkBFh8/hdOGR0T6Bll9TPO1NQB/UqMy/tKr3sA3KZm
XVDbRKSuUAQWz5J9/hLPmVMU41F/uD7mvyDY+x8GymInZjUXG4e0oq2RJgU6SYZ8
mkscM6qhKY3mL487w/kHVFtFlMkOhvI7LIh3zVvWwgGSAoAv9yai9BDZNFSk0cEb
bb/IK7BQW9sNI3lcnGirdbnjV94X9/sCAwEAATANBgkqhkiG9w0BAQsFAAOCAQEA
MJLeGdYO3dpsPx2R39Bw0qa5cUh42huPf8n7rp4a4Ca5jJjcAlCYV8HzqOzpiKYy
ZNuHy8LnNVYYh5Qoh8EO45bplMV1wnHfi6hW6DY5j3SQdcxkoVsW5R7rBF7a7SDg
6uChVRPHgsnALUUc7Wvvd3sAs/NKHzHu86mgD3EefkdqWAaCapzcqT9mo9KXkWJM
DhSJS+/iIaroc8umDnbPfhhgnlMf0/D4q0TjiLSSqyLzVifxnv9yHz56TrhHG/QP
E/8+FEGCHYKM4JLr5smGlzv72Kfx9E1CkG6TgFNIHjipVv1AtYDvaNMdPF2533+F
wE3YmpC3Q0g9r44nEbz4Bw==
-----END CERTIFICATE-----`
	)

	// Define TLS configuration
	certificate, err := tls.X509KeyPair([]byte(certPEM), []byte(keyPEM))
	if err != nil {
		log.Println(err)
	}

	caCertPool := x509.NewCertPool()
	if ok := caCertPool.AppendCertsFromPEM([]byte(caPEM)); !ok {
		log.Println(err)
	}

	tlsConfig := &tls.Config{
		InsecureSkipVerify: true, // Set this to false in production with valid certificates
		Certificates:       []tls.Certificate{certificate},
		RootCAs:            caCertPool,
	}

	p.newPostReader = kafka.NewReader(kafka.ReaderConfig{
		Brokers:   brokers,
		Topic:     topic1,
		GroupID:   "service.publisher",
		Partition: 0,
		MinBytes:  10e3,
		MaxBytes:  10e6,
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

func (p *PublisherKafkaGo) Run() {
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
