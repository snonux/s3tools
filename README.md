# s3rm

`s3rm` is an interactive and batch-friendly tool for safely deleting selected
directory prefixes from S3-compatible object storage.

The initial release will support AWS S3, Ceph RADOS Gateway, and Garage.

## Development

```sh
mage build
mage test
```

## Docker

Build the image from the repository root:

```sh
docker build -t s3rm .
```

The runtime image includes CA certificates for HTTPS endpoints and runs as the
unprivileged `s3rm` user (UID/GID 10001). Its default configuration path is
`/home/s3rm/.config/s3rm/config.toml`.

Mount a TOML configuration file at that path:

```sh
docker run --rm -it \
  -v "$PWD/config.toml:/home/s3rm/.config/s3rm/config.toml:ro" \
  s3rm
```

The mounted file must be readable by UID/GID `10001`; adjust its ownership or
mode when it is a host file with restrictive permissions.

For example, `config.toml` can contain the bucket and endpoint settings:

```toml
bucket = "photos"
region = "eu-west-1"
endpoint = "https://s3.example.com"
exclude = ["keep/"]
```

Credentials can be passed through the standard AWS environment variables. The
S3-specific values may also be supplied with `S3RM_BUCKET`, `S3RM_REGION`, and
`S3RM_ENDPOINT`; environment values override configuration-file values.

```sh
docker run --rm -it \
  -e S3RM_BUCKET=photos \
  -e S3RM_REGION=eu-west-1 \
  -e S3RM_ENDPOINT=https://s3.example.com \
  -e AWS_ACCESS_KEY_ID \
  -e AWS_SECRET_ACCESS_KEY \
  -e AWS_SESSION_TOKEN \
  s3rm
```

The `-it` options select interactive mode, where `s3rm` lists prefixes and
asks for the bucket name before deletion. For non-interactive use, omit `-it`
and supply every target prefix plus the exact configured bucket name:

```sh
docker run --rm \
  -e S3RM_BUCKET=photos \
  -e S3RM_REGION=eu-west-1 \
  -e S3RM_ENDPOINT=https://s3.example.com \
  -e AWS_ACCESS_KEY_ID \
  -e AWS_SECRET_ACCESS_KEY \
  s3rm --prefix=2025/ --prefix=exports/ --confirm-bucket=photos
```
