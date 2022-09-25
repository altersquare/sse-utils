FROM alpine:latest

# Install dependencies
RUN apk update
RUN apk add \
poppler-utils=0.88.0-r0 \
--repository=http://dl-cdn.alpinelinux.org/alpine/v3.12/main

ENTRYPOINT [ "pdftotext" ]