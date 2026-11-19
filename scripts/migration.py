#!/usr/bin/env python3
"""Copy live objects between two s3ite HTTP endpoints.

Install boto3 with `python3 -m pip install 'boto3>=1.36'`, then run

    python3 scripts/migration.py --source http://localhost:8000 --destination http://localhost:8001

Both clients use the standard AWS credential chain. If the servers use different
credentials, pass --source-profile and --destination-profile with the names of
your configured AWS profiles.

Run the old server read-only for a consistent copy. Allow temporary disk space
for the largest source object. The script checks the legacy MD5 when available
and reads back each destination object to compare SHA-256 and metadata. A rerun
accepts identical objects. A key that conflicts or fails is reported and
skipped, never overwritten, and the run continues. After each bucket the script
compares the destination's keys with the source's. It exits non-zero and lists
every problem if anything did not match. The destination assigns new
timestamps. Only current objects and their metadata are copied.

A v0.6.0 bucket containing a directory marker (an empty object whose key ends
in a slash) can return InternalError during listing. The script reports that
failure because the old HTTP endpoint cannot enumerate the bucket.
"""

import argparse
from base64 import b64encode
import hashlib
import re
import sys
from contextlib import closing
from tempfile import TemporaryFile

import boto3
from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError
from botocore.handlers import set_list_objects_encoding_type_url


CHUNK_SIZE = 1024 * 1024
CONTENT_HEADERS = (
    "CacheControl", "ContentDisposition", "ContentEncoding", "ContentLanguage",
    "ContentType", "Expires",
)


def client(endpoint, profile):
    return boto3.Session(profile_name=profile).client(
        "s3",
        endpoint_url=endpoint,
        region_name="us-east-1",
        config=Config(
            signature_version="s3v4",
            s3={"addressing_style": "path"},
            request_checksum_calculation="when_required",
        ),
    )


def object_keys(source, bucket):
    # v0.6 can reuse the same continuation token across successive pages.
    params = {"Bucket": bucket}
    last_key = None
    while True:
        page = source.list_objects_v2(**params)
        entries = page.get("Contents", [])
        for entry in entries:
            key = entry["Key"]
            if last_key is not None and key <= last_key:
                raise ValueError(f"listing did not advance after {last_key!r}")
            yield key
            last_key = key
        if not page["IsTruncated"]:
            return
        if not entries or not page.get("NextContinuationToken"):
            raise ValueError("truncated object listing has no entries or continuation token")
        params["ContinuationToken"] = page["NextContinuationToken"]


def fingerprint(response, output=None, expected_md5=None):
    md5 = hashlib.md5(usedforsecurity=False) if expected_md5 else None
    digest = hashlib.sha256()
    size = 0
    with closing(response["Body"]) as body:
        for chunk in body.iter_chunks(CHUNK_SIZE):
            if output is not None:
                output.write(chunk)
            if md5 is not None:
                md5.update(chunk)
            digest.update(chunk)
            size += len(chunk)
    if size != response["ContentLength"]:
        raise ValueError(f"received {size} bytes, expected {response['ContentLength']}")
    if md5 is not None and md5.hexdigest() != expected_md5:
        raise ValueError("source content differs from its stored MD5 ETag")
    return digest.digest()


def copy_object(source, destination, bucket, key):
    address = {"Bucket": bucket, "Key": key}
    response = source.get_object(**address)
    if "ExpiresString" in response and "Expires" not in response:
        # botocore drops an Expires value it cannot parse as a date. The new
        # server only stores HTTP dates, so copying would silently lose it.
        raise ValueError(
            f"Expires {response['ExpiresString']!r} is not an HTTP date and cannot be copied"
        )
    metadata = {"Metadata": response.get("Metadata", {})}
    metadata.update({name: response[name] for name in CONTENT_HEADERS if name in response})
    with TemporaryFile() as staged:
        # v0.6 stores a whole-object MD5, including for completed multipart uploads.
        # A composite multipart ETag from a newer server has a different format.
        etag = response.get("ETag", "").strip('"').lower()
        expected_md5 = etag if re.fullmatch(r"[0-9a-f]{32}", etag) else None
        expected = fingerprint(response, staged, expected_md5)
        try:
            existing = destination.get_object(**address)
        except ClientError as error:
            if error.response["Error"]["Code"] != "NoSuchKey":
                raise
            staged.seek(0)
            # Every v0.6 object fits in a single PUT. The condition also protects
            # a destination key created between the GET and the PUT.
            destination.put_object(
                **address, **metadata, Body=staged, IfNoneMatch="*",
                ChecksumAlgorithm="SHA256", ChecksumSHA256=b64encode(expected).decode("ascii"),
            )
            existing = destination.get_object(**address)
            copied = True
        else:
            copied = False

        if fingerprint(existing) != expected or any(
            existing.get(name) != value for name, value in metadata.items()
        ):
            raise ValueError("destination content or metadata differs from the source")
        return copied


def bucket_names(s3):
    for page in s3.get_paginator("list_buckets").paginate():
        for bucket in page.get("Buckets", []):
            yield bucket["Name"]


def migrate(source, destination):
    # v0.6 echoes EncodingType=url but returns literal keys. Disable boto3's
    # automatic encoding request so a key such as '%2F' survives unchanged.
    source.meta.events.unregister(
        "before-parameter-build.s3.ListObjectsV2", set_list_objects_encoding_type_url
    )
    # Also disable it for the destination, whose keys are compared with the source's.
    destination.meta.events.unregister(
        "before-parameter-build.s3.ListObjectsV2", set_list_objects_encoding_type_url
    )
    problems = []
    known_buckets = set(bucket_names(destination))
    for bucket in bucket_names(source):
        try:
            if bucket not in known_buckets:
                destination.create_bucket(Bucket=bucket)
            migrate_bucket(source, destination, bucket, problems)
        except (BotoCoreError, ClientError, ValueError) as error:
            problems.append(f"bucket {bucket!r}: {error}")
            print(f"{bucket}: stopped: {error}", flush=True)
    return problems


def migrate_bucket(source, destination, bucket, problems):
    """Copies one bucket's keys, then checks the destination holds exactly those keys.

    A failed key is recorded in `problems` and the copy continues with the next
    key. Listing errors propagate, because the bucket cannot then be verified.
    """
    copied = matched = failed = 0
    source_keys = set()
    for key in object_keys(source, bucket):
        source_keys.add(key)
        try:
            if copy_object(source, destination, bucket, key):
                copied += 1
            else:
                matched += 1
        except (BotoCoreError, ClientError, OSError, ValueError) as error:
            failed += 1
            problems.append(f"bucket {bucket!r} key {key!r}: {error}")

    destination_keys = set(object_keys(destination, bucket))
    missing = sorted(source_keys - destination_keys)
    extra = sorted(destination_keys - source_keys)
    for key in missing:
        problems.append(f"bucket {bucket!r} key {key!r}: missing from the destination")
    for key in extra:
        problems.append(f"bucket {bucket!r} key {key!r}: exists only on the destination")
    print(
        f"{bucket}: copied {copied}, already identical {matched}, failed {failed}, "
        f"source keys {len(source_keys)}, destination keys {len(destination_keys)}",
        flush=True,
    )


def main():
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--source", required=True, help="old S3 endpoint URL")
    parser.add_argument("--destination", required=True, help="new S3 endpoint URL")
    parser.add_argument("--source-profile", help="AWS profile for the old endpoint")
    parser.add_argument("--destination-profile", help="AWS profile for the new endpoint")
    args = parser.parse_args()
    if args.source.rstrip("/") == args.destination.rstrip("/"):
        parser.error("source and destination must be different endpoints")
    try:
        source = client(args.source, args.source_profile)
        destination = client(args.destination, args.destination_profile)
        problems = migrate(source, destination)
    except (BotoCoreError, ClientError, OSError, ValueError) as error:
        sys.exit(f"Migration stopped: {error}")
    if problems:
        print(f"Migration finished with {len(problems)} problem(s):", file=sys.stderr)
        for problem in problems:
            print(f"  {problem}", file=sys.stderr)
        sys.exit(1)
    print("Migration finished: every source key matches the destination.")


if __name__ == "__main__":
    main()
