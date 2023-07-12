#===============================
# Go commands
#===============================
tidy:
	# export GOPROXY="https://mirrors.tencent.com/go,https://proxy.golang.org,direct"
	export GOPROXY="https://proxy.golang.org,direct"
	# export GOPROXY="https://goproxy.cn,https://proxy.golang.org,direct"
	# export GOPROXY="https://proxy.golang.com.cn,https://proxy.golang.org,direct"
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