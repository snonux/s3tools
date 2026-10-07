# s3rm

`s3rm` safely deletes selected top-level prefixes from an S3-compatible
bucket. It supports AWS S3, Ceph RADOS Gateway, and Garage.

It runs interactively in a terminal or non-interactively in containers and
automation. Protected prefixes, exact bucket confirmation, progress, and final
verification help prevent accidental deletion.

## Quick start

Configure the bucket in `~/.config/s3rm/config.toml`, or set `S3RM_BUCKET`,
`S3RM_REGION`, and credentials in the environment, then run:

```sh
s3rm
```

For automation, require an explicit prefix and matching bucket confirmation:

```sh
s3rm --prefix=old-exports/ --confirm-bucket=photos
```

See the [usage guide](docs/usage.md) for configuration, exclusions, Docker,
progress reporting, and non-interactive examples.

## Development

```sh
mage test
mage build
mage check
```

`mage check` runs `go vet ./...`, `go test -race ./...`, checks that
`gofmt -l .` reports no files, and runs `errcheck ./...`. It requires Go,
Mage, and [errcheck](https://github.com/kisielk/errcheck) on `PATH`. Install
errcheck with:

```sh
go install github.com/kisielk/errcheck@latest
```
