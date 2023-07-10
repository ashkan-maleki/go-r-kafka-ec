#===============================
# Go commands
#===============================
tidy:
	go mod tidy
	go mod vendor

run:
	go run cmd/main.go

up: compose-up tidy run


#===============================
# Git commands
#===============================

commit:
	git add .
	git commit -m "$(CMSG)"
	git push

#===============================
# Compose commands
#===============================

compose-up:
	docker compose -f docker-compose.yaml up -d

compose-down:
	docker compose -f docker-compose.yaml down