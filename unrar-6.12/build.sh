#!/bin/sh
docker image rm -f rohandhamapurkar/unrar:6.12

docker build -t rohandhamapurkar/unrar:6.12 .