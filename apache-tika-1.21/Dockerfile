FROM alpine:latest

# Install dependencies
RUN apk update
RUN apk add --virtual wget
RUN apk add openjdk8-jre

RUN wget https://repo1.maven.org/maven2/org/apache/tika/tika-app/1.21/tika-app-1.21.jar -O /tmp/tika-app.jar

ENTRYPOINT [ "java", "-jar", "/tmp/tika-app.jar" ]