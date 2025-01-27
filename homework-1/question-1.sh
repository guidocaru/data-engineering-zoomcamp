#!/bin/bash

# Pull image
docker pull python:3.12.8

# Run the image and check pip version
docker run -it --entrypoint bash python:3.12.8 -c "pip --version"

# Exit the container
exit