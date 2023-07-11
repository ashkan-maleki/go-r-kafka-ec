package main

import (
	"errors"
	"log"

	// _ "github.com/gofiber/fiber/v2"
	"github.com/labstack/echo/v4"
	"github.com/mamalmaleki/go-r-kafka-ec/internal/service"
	"github.com/mamalmaleki/go-r-kafka-ec/internal/web"
	"github.com/redis/go-redis/v9"
)

func main() {
	service.KafkaSetup()
	cm, closeCacheManager := service.NewCacheManager2()
	defer closeCacheManager()
	p, closePublisher := service.NewPublisher2()
	defer closePublisher()
	api, closeAPI := web.NewAPI2()
	defer closeAPI()

	// run background services
	go cm.Run2()
	go p.Run2()

	// setup HTTP server
	e := echo.New()
	e.POST("/post", func(c echo.Context) error {
		title := c.Request().PostFormValue("title")
		content := c.Request().PostFormValue("content")
		_, err := api.NewMessage2(title, content)
		if err != nil {
			return c.String(500, err.Error())
		}
		return c.String(201, "We have received your post and it will be published sooner or later.")
	})

	e.GET("/post/:slug", func(c echo.Context) error {
		log.Println("hi post slug")
		post, err := api.GetPost(c.Param("slug"))
		if err != nil {
			if errors.Is(err, redis.Nil) {
				return c.String(404, "not found")
			}
			return c.String(500, err.Error())
		}
		return c.JSON(200, post)
	})
	go e.Logger.Fatal(e.Start(":1323"))
}
