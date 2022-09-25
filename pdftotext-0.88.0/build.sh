#!/bin/sh
docker image rm -f rohandhamapurkar/pdftotext:0.88.0

docker build -t rohandhamapurkar/pdftotext:0.88.0 .