.PHONY: build-image

build-image:
	docker build -t functionstream/fs-python-base .

test:
	PYTHONPATH=. python -m pytest