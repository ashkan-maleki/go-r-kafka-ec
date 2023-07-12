# A simple blog has been built using Kafka, Golang, and Redis
A) Save a blog post:
    1) The save api is called
    2) The save method sends a message to kafka
    3) A kafka reader receives a message, stores the blog post, and sends a another message to kafka (using another topic)
    4) A kafka reader receives a message (from another topic) and stores it in Redis

B) Get a blog post
    1) The get api is called and the get method retrieves the blog post from Redis

# Requirements
1) Kafka, 2) Golang, 3) Redis, 4) Postgres, 6) Docker, 7) Docker Compose, 8) Make, 9) Gofiber