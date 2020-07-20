FROM golang:1.14-alpine AS build
RUN apk add --update git
WORKDIR /work
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN go build

FROM alpine
RUN apk add --update --no-cache ca-certificates
COPY --from=build /work/ddb-sync /usr/local/bin/ddb-sync
ENTRYPOINT ["ddb-sync"]