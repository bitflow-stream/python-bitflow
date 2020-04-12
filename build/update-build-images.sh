#!/bin/bash

docker build -t bitflowstream/python-build:debian -f build.Dockerfile .
docker push bitflowstream/python-build:debian
