#!/usr/bin/env bash
# Probe readiness and round-trip an object against a fresh S3 endpoint.
set -euo pipefail

endpoint=${1:?S3 endpoint required}
health=${2:?health endpoint required}
tmp_dir=$(mktemp -d)
trap 'rm -rf "$tmp_dir"' EXIT

curl --fail --silent --show-error --retry 30 --retry-all-errors --retry-delay 1 \
    --max-time 2 --retry-max-time 60 "$health/readyz"

bucket=s3ite-image-smoke
s3=(aws --endpoint-url "$endpoint" --no-cli-pager s3api)
printf 's3ite image smoke test\n' > "$tmp_dir/input"
"${s3[@]}" create-bucket --bucket "$bucket" >/dev/null
"${s3[@]}" put-object --bucket "$bucket" --key smoke --body "$tmp_dir/input" >/dev/null
"${s3[@]}" get-object --bucket "$bucket" --key smoke "$tmp_dir/output" >/dev/null
cmp "$tmp_dir/input" "$tmp_dir/output"
