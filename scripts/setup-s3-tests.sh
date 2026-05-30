#!/usr/bin/env bash
set -Eeuo pipefail

usage() {
  cat <<'EOF'
Usage:
  scripts/setup-s3-tests.sh [--run-curated [pytest-args...]]
  scripts/setup-s3-tests.sh --run <pytest-args...>

Installs uv if needed, creates a local Python environment for clone/s3-tests,
and writes disposable s3ite/s3-tests config files.

Environment overrides:
  S3TESTS_DIR       Path to Ceph s3-tests clone (default: clone/s3-tests)
  S3TESTS_VENV      Python venv path (default: .venv-s3tests)
  S3TESTS_PYTHON    Python version for uv venv (default: 3.12)
  S3TEST_CONF       s3-tests config path (default: .s3tests/s3ite.conf)
  S3ITE_CONF        s3ite config path (default: .s3tests/s3ite-server.yaml)
  S3ITE_ROOT        s3ite data root (default: .s3tests/data)
  S3ITE_HOST        s3ite host (default: 127.0.0.1)
  S3ITE_PORT        s3ite port (default: 8014)
  S3ITE_ACCESS_KEY  access key for all test users (default: s3ite-access-key)
  S3ITE_SECRET_KEY  secret key for all test users

Run examples:
  cargo run --features binary -- --config .s3tests/s3ite-server.yaml
  scripts/setup-s3-tests.sh --run-curated
  scripts/setup-s3-tests.sh --run clone/s3-tests/s3tests/functional/test_s3.py::test_object_write_read_update_read_delete
EOF
}

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)"

mode="setup"
pytest_args=()
case "${1:-}" in
  -h|--help)
    usage
    exit 0
    ;;
  --run-curated)
    mode="curated"
    shift
    pytest_args=("$@")
    ;;
  --run)
    mode="custom"
    shift
    pytest_args=("$@")
    ;;
  "")
    ;;
  *)
    printf 'unexpected argument: %s\n\n' "$1" >&2
    usage >&2
    exit 2
    ;;
esac

if [[ "$mode" == "custom" && "${#pytest_args[@]}" -eq 0 ]]; then
  printf '%s\n' '--run requires at least one pytest argument' >&2
  exit 2
fi

s3tests_dir="${S3TESTS_DIR:-$repo_root/clone/s3-tests}"
venv_dir="${S3TESTS_VENV:-$repo_root/.venv-s3tests}"
python_version="${S3TESTS_PYTHON:-3.12}"
generated_dir="$repo_root/.s3tests"
s3test_conf="${S3TEST_CONF:-$generated_dir/s3ite.conf}"
s3ite_conf="${S3ITE_CONF:-$generated_dir/s3ite-server.yaml}"
s3ite_root="${S3ITE_ROOT:-$generated_dir/data}"
s3ite_host="${S3ITE_HOST:-127.0.0.1}"
s3ite_port="${S3ITE_PORT:-8014}"
s3ite_access_key="${S3ITE_ACCESS_KEY:-s3ite-access-key}"
s3ite_secret_key="${S3ITE_SECRET_KEY:-s3ite-secret-key-for-local-conformance}"
uv_install_dir="${UV_INSTALL_DIR:-$repo_root/.local/bin}"

if [[ ! -d "$s3tests_dir" ]]; then
  cat >&2 <<EOF
Missing s3-tests clone: $s3tests_dir

Clone it first, for example:
  git clone https://github.com/ceph/s3-tests.git "$s3tests_dir"
EOF
  exit 1
fi

find_uv() {
  if [[ -n "${UV:-}" && -x "${UV:-}" ]]; then
    printf '%s\n' "$UV"
  elif command -v uv >/dev/null 2>&1; then
    command -v uv
  elif [[ -x "$uv_install_dir/uv" ]]; then
    printf '%s\n' "$uv_install_dir/uv"
  fi
}

uv_bin="$(find_uv || true)"
if [[ -z "$uv_bin" ]]; then
  mkdir -p "$uv_install_dir"
  printf 'Installing uv into %s\n' "$uv_install_dir"
  if command -v curl >/dev/null 2>&1; then
    curl -LsSf https://astral.sh/uv/install.sh | env UV_UNMANAGED_INSTALL="$uv_install_dir" sh
  elif command -v wget >/dev/null 2>&1; then
    wget -qO- https://astral.sh/uv/install.sh | env UV_UNMANAGED_INSTALL="$uv_install_dir" sh
  else
    printf 'curl or wget is required to install uv\n' >&2
    exit 1
  fi
  uv_bin="$(find_uv || true)"
fi

if [[ -z "$uv_bin" || ! -x "$uv_bin" ]]; then
  printf 'uv installation did not produce an executable uv binary\n' >&2
  exit 1
fi

mkdir -p "$generated_dir" "$s3ite_root"

if [[ -x "$venv_dir/bin/python" ]]; then
  printf 'Using existing Python environment at %s\n' "$venv_dir"
else
  printf 'Creating Python environment at %s\n' "$venv_dir"
  "$uv_bin" venv --python "$python_version" "$venv_dir"
fi

UV_LINK_MODE="${UV_LINK_MODE:-copy}" "$uv_bin" pip install --python "$venv_dir/bin/python" \
  -r "$s3tests_dir/requirements.txt" \
  -e "$s3tests_dir"

cat >"$s3ite_conf" <<EOF
root: $s3ite_root
host: $s3ite_host
port: $s3ite_port
access_key: $s3ite_access_key
secret_key: $s3ite_secret_key
permissive_cors: true
concurrency_limit: 16
readers: 4
read_only: false
journal_mode: WAL
synchronous: NORMAL
temp_store: MEMORY
cache_size: 65536
EOF

cat >"$s3test_conf" <<EOF
[DEFAULT]
host = $s3ite_host
port = $s3ite_port
is_secure = False
ssl_verify = False

[fixtures]
bucket prefix = s3ite-{random}-
iam name prefix = s3ite-iam-{random}-
iam path prefix = /s3ite-tests/

[s3 main]
display_name = s3ite
user_id = s3ite-main
email = s3ite-main@example.com
api_name = default
access_key = $s3ite_access_key
secret_key = $s3ite_secret_key

[s3 alt]
display_name = s3ite-alt
user_id = s3ite-alt
email = s3ite-alt@example.com
access_key = $s3ite_access_key
secret_key = $s3ite_secret_key

[s3 tenant]
display_name = testx\$s3ite-tenant
user_id = testx\$s3ite-tenant
email = s3ite-tenant@example.com
tenant = testx
access_key = $s3ite_access_key
secret_key = $s3ite_secret_key

[iam]
display_name = s3ite-iam
user_id = s3ite-iam
email = s3ite-iam@example.com
access_key = $s3ite_access_key
secret_key = $s3ite_secret_key

[iam root]
account_id = S3ITE1111111111111
user_id = S3ITE1111111111111
email = s3ite-root@example.com
access_key = $s3ite_access_key
secret_key = $s3ite_secret_key

[iam alt root]
account_id = S3ITE2222222222222
user_id = S3ITE2222222222222
email = s3ite-alt-root@example.com
access_key = $s3ite_access_key
secret_key = $s3ite_secret_key
EOF

curated_tests=(
  "$s3tests_dir/s3tests/functional/test_s3.py::test_object_write_read_update_read_delete"
  "$s3tests_dir/s3tests/functional/test_s3.py::test_object_head_zero_bytes"
  "$s3tests_dir/s3tests/functional/test_s3.py::test_bucket_listv2_delimiter_basic"
  "$s3tests_dir/s3tests/functional/test_s3.py::test_bucket_listv2_delimiter_prefix"
  "$s3tests_dir/s3tests/functional/test_s3.py::test_bucket_listv2_delimiter_prefix_underscore"
  "$s3tests_dir/s3tests/functional/test_s3.py::test_bucket_listv2_delimiter_percentage"
  "$s3tests_dir/s3tests/functional/test_s3.py::test_bucket_listv2_continuationtoken"
  "$s3tests_dir/s3tests/functional/test_s3.py::test_ranged_request_response_code"
  "$s3tests_dir/s3tests/functional/test_s3.py::test_ranged_request_skip_leading_bytes_response_code"
  "$s3tests_dir/s3tests/functional/test_s3.py::test_ranged_request_return_trailing_bytes_response_code"
  "$s3tests_dir/s3tests/functional/test_s3.py::test_ranged_request_invalid_range"
  "$s3tests_dir/s3tests/functional/test_s3.py::test_ranged_request_empty_object"
  "$s3tests_dir/s3tests/functional/test_s3.py::test_object_copy_same_bucket"
  "$s3tests_dir/s3tests/functional/test_s3.py::test_object_copy_diff_bucket"
  "$s3tests_dir/s3tests/functional/test_s3.py::test_multipart_upload_missing_part"
  "$s3tests_dir/s3tests/functional/test_s3.py::test_multipart_upload_incorrect_etag"
  "$s3tests_dir/s3tests/functional/test_s3.py::test_multipart_upload_multiple_sizes"
)

if [[ "$mode" == "curated" || "$mode" == "custom" ]]; then
  if [[ "$mode" == "curated" ]]; then
    pytest_args=("${curated_tests[@]}" "${pytest_args[@]}")
  fi

  "$venv_dir/bin/python" - "$s3ite_host" "$s3ite_port" <<'PY'
import socket
import sys

host = sys.argv[1]
port = int(sys.argv[2])
try:
    with socket.create_connection((host, port), timeout=1):
        pass
except OSError:
    print(f"s3ite does not appear to be listening on {host}:{port}", file=sys.stderr)
    print("Start it first with:", file=sys.stderr)
    print("  cargo run --features binary -- --config .s3tests/s3ite-server.yaml", file=sys.stderr)
    sys.exit(1)
PY

  cd "$repo_root"
  S3TEST_CONF="$s3test_conf" "$venv_dir/bin/python" -m pytest "${pytest_args[@]}"
  exit $?
fi

cat <<EOF

s3-tests setup complete.

uv:           $uv_bin
venv:         $venv_dir
s3ite config: $s3ite_conf
s3-tests conf:$s3test_conf

Start s3ite:
  cargo run --features binary -- --config "$s3ite_conf"

Run the curated subset in another shell:
  scripts/setup-s3-tests.sh --run-curated

Run one test:
  scripts/setup-s3-tests.sh --run "$s3tests_dir/s3tests/functional/test_s3.py::test_object_write_read_update_read_delete"
EOF
