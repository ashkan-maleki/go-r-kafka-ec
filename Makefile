#===============================
# Go commands
#===============================
tidy:
	go mod tidy
	go mod vendor

run:
	go run cmd/main.go


#===============================
# Git commands
#===============================

git-commit:
	git add .
	git commit -m "$(CMSG)"
	git push
