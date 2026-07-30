REPO	:= sci-docker.ncats.nih.gov/rdas
NAME	:= ${REPO}/nhs_info_extraction
TAG		:= $$(git log -1 --pretty=%h)
IMG		:= ${NAME}:${TAG}
LATEST 	:= ${NAME}:latest

build:
	@docker build --platform linux/amd64 -t ${IMG} .
	@docker tag ${IMG} ${LATEST}

push:
	@docker push --all-tags ${NAME}

login:
	@docker login ${REPO} -u ${DOCKER_USER} -p ${DOCKER_PASS}

# Reclaim local disk space by removing the images built above and pruning the
# build cache. Safe to run after the images are pushed to the registry.
clean:
	@docker rmi ${IMG} ${LATEST} 2>/dev/null || true
	@docker builder prune -f

# Build, push, then free local disk space in one step.
release: build push clean

.PHONY: build push login clean release