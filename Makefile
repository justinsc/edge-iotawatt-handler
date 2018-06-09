DOCKER_IMAGE_VERSION=1.0.0
DOCKER_IMAGE_NAME=tdmproject/edge-iotawatt-handler
DOCKER_IMAGE_TAGNAME=$(DOCKER_IMAGE_NAME):$(DOCKER_IMAGE_VERSION)

default: build

build:
	docker build -f docker/Dockerfile -t $(DOCKER_IMAGE_TAGNAME) .
	docker tag $(DOCKER_IMAGE_TAGNAME) $(DOCKER_IMAGE_NAME):latest

push:
	docker push $(DOCKER_IMAGE_TAGNAME)
	docker push $(DOCKER_IMAGE_NAME)

test:
	docker run --rm --entrypoint=/bin/echo $(DOCKER_IMAGE_TAGNAME) "Success."
