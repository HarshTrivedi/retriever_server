#!/usr/bin/env bash

docker run -it --init --rm --network host ekzhang/bore local 8000 --to bore.pub
