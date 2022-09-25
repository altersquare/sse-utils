#!/bin/sh
docker image rm -f rohandhamapurkar/apache-tika:1.21

docker build -t rohandhamapurkar/apache-tika:1.21 .