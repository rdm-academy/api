PROG_NAME := project
IMAGE_NAME := quay.io/rdm-academy/$(PROG_NAME)

GIT_SHA := $(or $(shell git log -1 --pretty=format:"%h"), "latest")
GIT_TAG := $(shell git describe --tags --exact-match 2>/dev/null)
GIT_VERSION := $(shell git log -1 --pretty=format:"%h (%ci)")

ifndef BRANCH_NAME
	GIT_BRANCH := $(shell git symbolic-ref -q --short HEAD)
else
	GIT_BRANCH := $(BRANCH_NAME)
endif

GOOS := $(shell go env GOOS)
GOARCH := $(shell go env GOARCH)

nop:
	echo "No default target; pick one"

deps:
	go get -u github.com/golang/dep/...
	cd ./cmd/svc && dep ensure
	cd ./cmd/cli && dep ensure

proto:
	protoc --go_out=. service.proto
	protoc --plugin=protoc-gen-custom=$(GOPATH)/bin/nats-rpc --custom_out=. service.proto
	protoc --plugin=protoc-gen-custom=$(GOPATH)/bin/nats-rpc-cli --custom_out=cmd/cli service.proto

build:
	mkdir -p dist

	go build \
		-ldflags "-X \"main.buildVersion=$(GIT_VERSION)\" -extldflags -static" \
		-o ./dist/$(GOOS)-$(GOARCH)/$(PROG_NAME)-svc ./cmd/svc

	go build \
		-ldflags "-X \"main.buildVersion=$(GIT_VERSION)\" -extldflags -static" \
		-o ./dist/$(GOOS)-$(GOARCH)/$(PROG_NAME)-cli ./cmd/cli

docker:
	docker build -t ${IMAGE_NAME}:${GIT_SHA} .
	docker tag ${IMAGE_NAME}:${GIT_SHA} ${IMAGE_NAME}:${GIT_BRANCH}

	if [ "${GIT_TAG}" != "" ] ; then \
		docker tag ${IMAGE_NAME}:${GIT_SHA} ${IMAGE_NAME}:${GIT_TAG} ; \
	fi;

	if [ "${GIT_BRANCH}" == "master" ]; then \
		docker tag ${IMAGE_NAME}:${GIT_SHA} ${IMAGE_NAME}:latest ; \
	fi;

docker-push:
	docker push ${IMAGE_NAME}:${GIT_SHA}
	docker push ${IMAGE_NAME}:${GIT_BRANCH}

	if [ "${GIT_TAG}" != "" ]; then \
		docker push ${IMAGE_NAME}:${GIT_TAG} ; \
	fi;

	if [ "${GIT_BRANCH}" == "master" ]; then \
		docker push ${IMAGE_NAME}:latest ; \
	fi;

.PHONY: nop build proto docker docker-push
