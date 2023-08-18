package main

import (
	"context"
	"mymodule/redisbroker"

	"github.com/go-redis/redis/v8"
	"go.uber.org/zap"
)

func main() {
	ctx := context.Background()
	logger, _ := zap.NewDevelopment()
	redisClient := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})

	redisBroker := redisbroker.New(ctx, redisClient, logger)
	go redisBroker.Listen()
	myChan := redisBroker.Subscribe(ctx, "stromme/han/live/RygX3luy")
	for m := range myChan {
		logger.Info("m", zap.ByteString("m", m))
	}
}
