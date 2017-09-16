FROM golang:1.9
WORKDIR /go/src/github.com/petertrotman/gdax-scraper
COPY . .
RUN go get github.com/Masterminds/glide
RUN glide install

# from https://docs.docker.com/engine/userguide/eng-image/multistage-build/#before-multi-stage-builds
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o app .

FROM alpine:latest
WORKDIR /root
COPY --from=0 /go/src/github.com/petertrotman/gdax-scraper/app .
CMD ["./app"]
