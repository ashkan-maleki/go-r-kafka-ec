#===============================
# Go commands
#===============================
tidy:
	go mod tidy
	go mod vendor

run:
	go run cmd/main.go


#===============================
# Git commands hi
#===============================

git-commit:
	git add .
	git commit -m "$(COMMIT_MSG)"
