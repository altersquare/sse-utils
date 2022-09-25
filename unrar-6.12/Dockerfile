FROM alpine:latest

# Install dependencies
RUN apk update && apk add --virtual \ 
build-dependencies \
build-base \
wget

RUN apk add \ 
libc6-compat

RUN wget https://www.win-rar.com/fileadmin/winrar-versions/rarlinux-x64-612.tar.gz -P /tmp

RUN tar -xvzf /tmp/rarlinux-x64-612.tar.gz -C /tmp
RUN cp /tmp/rar/unrar /usr/local/bin/unrar

RUN chmod +x /usr/local/bin/unrar

RUN rm -rf /tmp/rarlinux-x64-612.tar.gz
RUN rm -rf /tmp/rar

ENTRYPOINT [ "unrar" ]