FROM golang:1.26-bookworm AS build

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . ./
RUN CGO_ENABLED=0 go build -trimpath -ldflags='-s -w' -o /out/s3rm ./cmd/s3rm

FROM debian:bookworm-slim

RUN apt-get update \
    && apt-get install --no-install-recommends -y ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && addgroup --system --gid 10001 s3rm \
    && adduser --system --uid 10001 --ingroup s3rm --home /home/s3rm s3rm \
    && install -d -o s3rm -g s3rm /home/s3rm/.config/s3rm

COPY --from=build --chown=s3rm:s3rm /out/s3rm /usr/local/bin/s3rm

ENV HOME=/home/s3rm \
    XDG_CONFIG_HOME=/home/s3rm/.config

USER s3rm:s3rm
WORKDIR /home/s3rm

ENTRYPOINT ["s3rm"]
