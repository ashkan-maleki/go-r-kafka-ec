package main

import (
	echo "github.com/labstack/echo/v4"
	"github.com/mamalmaleki/go-r-kafka-ec/internal/service"
	"github.com/mamalmaleki/go-r-kafka-ec/internal/web"
)

func main() {
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
	e := echo.New()
	e.P
}
