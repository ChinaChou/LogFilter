FROM alpine:3.7
COPY ./bin/logfilter /
ENTRYPOINT /logfilter
