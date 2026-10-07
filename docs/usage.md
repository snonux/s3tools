# Using s3rm

`s3rm` deletes selected top-level prefixes from one S3-compatible bucket. It
supports AWS S3, Ceph RADOS Gateway, and Garage.

## Configure a connection

By default, `s3rm` reads `~/.config/s3rm/config.toml`. Use `-config PATH` to
select another file.

```toml
bucket = "photos"
region = "eu-west-1"
endpoint = "https://s3.example.com" # optional for AWS S3
exclude = ["keep/"]

# Optional static credentials. Prefer environment credentials when possible.
access_key_id = "..."
secret_access_key = "..."
session_token = "..."

# Custom endpoints use path-style addressing by default. Uncomment one only
# when the endpoint requires a different addressing style.
# force_path_style = true
# use_virtual_host_style = true
```

Environment values override the file for `S3RM_BUCKET`, `S3RM_REGION`, and
`S3RM_ENDPOINT`. Standard AWS variables are also supported:

```sh
export S3RM_BUCKET=photos
export S3RM_REGION=eu-west-1
export S3RM_ENDPOINT=https://garage.example.com
export AWS_ACCESS_KEY_ID=...
export AWS_SECRET_ACCESS_KEY=...
export AWS_SESSION_TOKEN=... # optional
```

Each S3 HTTP attempt has a two-minute timeout covering connection setup,
response headers, and reading the response body. The caller's context can end
an attempt sooner. The AWS SDK keeps its normal retry policy, so one API call
can include multiple bounded attempts. The timeout applies per attempt, so
listing and deleting a large bucket can take longer overall.

For a custom endpoint, `s3rm` uses path-style addressing unless
`use_virtual_host_style` is set. This works with Garage and is normally the
right choice for RADOS Gateway too.

## Interactive use

Run `s3rm` from a terminal with no prefix flags:

```sh
s3rm
```

The tool lists selectable top-level prefixes, marks protected prefixes as
unselectable, accepts a numbered multi-selection, then requires the exact
bucket name before it deletes anything. Root-level objects are never
selectable.

Use `s` at the selection prompt to stop an optional bucket-statistics scan.

## Non-interactive use

Non-interactive execution requires one or more complete top-level prefixes
and the exact configured bucket name:

```sh
s3rm \
  --prefix=2024-archive/ \
  --prefix=exports/ \
  --confirm-bucket=photos
```

Prefixes must end in `/`, exist at the top level, and not be entirely
protected. A root object or an omitted confirmation is rejected before any
deletion begins.

Add protection from the command line with repeatable `-exclude` flags. An
excluded trailing-slash path protects its whole subtree; an exact object-key
path protects only that key.

```sh
s3rm -exclude keep/ -exclude legal/hold.txt \
  --prefix=2024-archive/ --confirm-bucket=photos
```

## Progress and bucket statistics

`s3rm` reports its current phase immediately and refreshes status every two
seconds, including while an S3 request is waiting. This covers connection,
bucket discovery, optional bucket statistics, streaming deletion, and final
verification. Discovery shows completed pages and prefixes found. Deletion
starts as soon as listing finds eligible objects, with a bounded number of
pages queued while listing continues. Status shows discovered, deleted, and
known pending objects and bytes. The exact total is unknown until listing
finishes, so the progress bar and ETA appear only then. The final status says
`deletion complete` once verification succeeds. A terminal updates one line;
redirected output emits timestamped log lines. Interactive selection prompts
pause status output until the bucket name has been confirmed.

For unversioned buckets, listing and deletion run concurrently using a key
cursor. Versioned buckets delete one page before listing the next so version
markers cannot become invalid. Both modes verify the selected prefixes after
deletion. If another client keeps writing to a prefix, verification can report
remaining objects and the command exits with an error.

Use `--bucket-stats` in either interactive or batch mode to scan the whole
bucket before deletion:

```sh
s3rm --prefix=logs/ --confirm-bucket=my-bucket --bucket-stats \
  --bucket-stats-pages=1000 --bucket-stats-timeout=2m
```

The defaults are 1,000 pages and two minutes. When the scan completes, its
snapshot is the baseline for periodic whole-bucket remainder estimates; each
estimate subtracts confirmed deletions. It reflects scan time and is approximate
if other clients change the bucket concurrently. A stopped or bounded scan is
clearly labelled incomplete, and its remainder estimate is unavailable.

## Docker

Build the image:

```sh
docker build -t s3rm .
```

The image runs as UID/GID 10001 and reads its default config from
`/home/s3rm/.config/s3rm/config.toml`. A mounted config file must be readable
by that user.

```sh
docker run --rm -it \
  -v "$PWD/config.toml:/home/s3rm/.config/s3rm/config.toml:ro" \
  s3rm
```

For automation, pass configuration and credentials as environment variables:

```sh
docker run --rm \
  -e S3RM_BUCKET=photos \
  -e S3RM_REGION=eu-west-1 \
  -e S3RM_ENDPOINT=https://s3.example.com \
  -e AWS_ACCESS_KEY_ID \
  -e AWS_SECRET_ACCESS_KEY \
  s3rm --prefix=old-exports/ --confirm-bucket=photos
```

## Deletion semantics

Unversioned buckets delete current objects. Versioned and suspended buckets
delete every eligible object version and delete marker by exact version ID.
Objects under protected paths remain untouched. `s3rm` verifies each selected
prefix after deletion and returns a failure when eligible objects remain.
