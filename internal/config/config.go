package config

const (
	RedisUrl           = "redis://default@localhost:6379"
	PostgresDsn        = "postgres://postgres:postgres@localhost:5432/posts"
	KafkaBrokerAddress = "localhost:9092"
)

//redisClient := redis.NewClient(&redis.Options{
////Addr:     "localhost:6379",
//Addr:     "redisCache:6379",
//Password: "",
//DB:       0,
//})
