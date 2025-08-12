.PHONY: build-image

build-image:
	docker build -t functionstream/fs-python-base .

docker-buildx:
	docker buildx build --platform linux/amd64,linux/arm64 -t functionstream/fs-python-base .

test:
	PYTHONPATH=. python -m pytest