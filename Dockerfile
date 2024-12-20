#!/bin/bash

docker container run --rm \
    --name "jupyter" \
    -p 8888:8888 -p 4040:4040 -p 18080:18080 \
    -v "$(pwd)"/data:/data \
    -v "$(pwd"/jupyter/notebook:/Users/xaviervanausloos/notebook \
    jupyter/pyspark-notebook
