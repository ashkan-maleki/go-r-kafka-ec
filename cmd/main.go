package main

import (
	"errors"
	"log"

	"github.com/gofiber/fiber/v2"
	"github.com/mamalmaleki/go-r-kafka-ec/internal/service"
	"github.com/mamalmaleki/go-r-kafka-ec/internal/web"
	"github.com/redis/go-redis/v9"
)

func main() {
	// service.KafkaSetup()
	cm, closeCacheManager := service.NewCacheManager()
	defer closeCacheManager()
	p, closePublisher := service.NewPublisher()
	defer closePublisher()
	api, closeAPI := web.NewAPI()
	defer closeAPI()

	// run background services
	go cm.Run()
	go p.Run()

	// setup HTTP server
	app := fiber.New()

	app.Post("/post", func(c *fiber.Ctx) error {
		payload := struct {
			Title   string `json:"title"`
			Content string `json:"content"`
		}{}

		if err := c.BodyParser(&payload); err != nil {
			return err
		}
		_, err := api.NewMessage(payload.Title, payload.Content)
		if err != nil {
			return c.Status(500).SendString(err.Error())
		}
		return c.Status(201).SendString("We have received your post and it will be published sooner or later.")
	})

	app.Get("/post/:slug", func(c *fiber.Ctx) error {
		post, err := api.GetPost(c.Params("slug"))
		if err != nil {
			if errors.Is(err, redis.Nil) {
				return c.Status(404).SendString("not found")
			}
			return c.Status(500).SendString(err.Error())
		}
		return c.JSON(post)
	})

	log.Fatal(app.Listen(":3000"))
}
