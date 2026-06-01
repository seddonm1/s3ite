#!/usr/bin/env bash
set -Eeuo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)"
work_dir="${TMPDIR:-/tmp}/s3ite-s3-tests"

s3tests_dir="${S3TESTS_DIR:-$work_dir/s3-tests}"
venv_dir="${S3TESTS_VENV:-$work_dir/.venv-s3tests}"
python_version="${S3TESTS_PYTHON:-3.12}"
generated_dir="$work_dir/.s3tests"
cargo_target_dir="${CARGO_TARGET_DIR:-$work_dir/target}"
s3test_conf="${S3TEST_CONF:-$generated_dir/s3ite.conf}"
s3ite_conf="${S3ITE_CONF:-$generated_dir/s3ite-server.yaml}"
s3ite_root="${S3ITE_ROOT:-$generated_dir/data}"
s3ite_host="${S3ITE_HOST:-127.0.0.1}"
s3ite_port="${S3ITE_PORT:-8014}"
s3ite_access_key="${S3ITE_ACCESS_KEY:-s3ite-access-key}"
s3ite_secret_key="${S3ITE_SECRET_KEY:-s3ite-secret-key-for-local-conformance}"
s3ite_log="$generated_dir/s3ite.log"
uv_install_dir="${UV_INSTALL_DIR:-$work_dir/.local/bin}"

pytest_args=("$@")
if [[ "${#pytest_args[@]}" -eq 0 ]]; then
  pytest_args=("s3tests/functional")
fi

has_mark_expression=false
for arg in "${pytest_args[@]}"; do
  case "$arg" in
    -m|--markexpr|--markexpr=*)
      has_mark_expression=true
      ;;
  esac
done

if [[ "$has_mark_expression" == false ]]; then
  pytest_args=(-m "not fails_on_rgw and not fails_on_aws" "${pytest_args[@]}")
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

ensure_s3tests() {
  if [[ -f "$s3tests_dir/requirements.txt" && -d "$s3tests_dir/s3tests/functional" ]]; then
    printf 'Using s3-tests checkout at %s\n' "$s3tests_dir"
    return
  fi

  if [[ -e "$s3tests_dir" ]]; then
    printf 's3-tests path exists but does not look like a checkout: %s\n' "$s3tests_dir" >&2
    exit 1
  fi

  if ! command -v git >/dev/null 2>&1; then
    printf 'git is required to clone Ceph s3-tests\n' >&2
    exit 1
  fi

  mkdir -p "$(dirname "$s3tests_dir")"
  printf 'Cloning Ceph s3-tests into %s\n' "$s3tests_dir"
  git clone https://github.com/ceph/s3-tests.git "$s3tests_dir"
}

install_uv() {
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
}

write_s3ite_config() {
  cat >"$s3ite_conf" <<EOF
root: $s3ite_root
host: $s3ite_host
port: $s3ite_port
access_key: $s3ite_access_key
secret_key: $s3ite_secret_key
permissive_cors: false
EOF
}

append_auth_section() {
  local section="$1"
  shift

  {
    printf '\n[%s]\n' "$section"
    printf '%s\n' "$@"
    printf 'access_key = %s\n' "$s3ite_access_key"
    printf 'secret_key = %s\n' "$s3ite_secret_key"
  } >>"$s3test_conf"
}

write_s3tests_config() {
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
EOF

  append_auth_section "s3 main" \
    "display_name = s3ite" \
    "user_id = s3ite-main" \
    "email = s3ite-main@example.com"

  append_auth_section "s3 alt" \
    "display_name = s3ite-alt" \
    "user_id = s3ite-alt" \
    "email = s3ite-alt@example.com"

  append_auth_section "s3 tenant" \
    'display_name = testx$s3ite-tenant' \
    'user_id = testx$s3ite-tenant' \
    "email = s3ite-tenant@example.com" \
    "tenant = testx"

  append_auth_section "iam" \
    "display_name = s3ite-iam" \
    "user_id = s3ite-iam" \
    "email = s3ite-iam@example.com"

  append_auth_section "iam root" \
    "account_id = S3ITE1111111111111" \
    "user_id = S3ITE1111111111111" \
    "email = s3ite-root@example.com"

  append_auth_section "iam alt root" \
    "account_id = S3ITE2222222222222" \
    "user_id = S3ITE2222222222222" \
    "email = s3ite-alt-root@example.com"
}

connect_host() {
  case "$s3ite_host" in
    0.0.0.0|::)
      printf '127.0.0.1\n'
      ;;
    *)
      printf '%s\n' "$s3ite_host"
      ;;
  esac
}

port_is_open() {
  "$venv_dir/bin/python" - "$(connect_host)" "$s3ite_port" <<'PY'
import socket
import sys

host = sys.argv[1]
port = int(sys.argv[2])
try:
    with socket.create_connection((host, port), timeout=0.2):
        pass
except OSError:
    sys.exit(1)
PY
}

wait_for_server() {
  local attempts=100
  local host
  host="$(connect_host)"

  for _ in $(seq 1 "$attempts"); do
    if ! kill -0 "$server_pid" 2>/dev/null; then
      printf 's3ite exited before it became ready. Log follows:\n' >&2
      sed -n '1,160p' "$s3ite_log" >&2 || true
      exit 1
    fi

    if port_is_open; then
      return
    fi

    sleep 0.1
  done

  printf 'timed out waiting for s3ite on %s:%s. Log follows:\n' "$host" "$s3ite_port" >&2
  sed -n '1,160p' "$s3ite_log" >&2 || true
  exit 1
}

server_pid=""
cleanup() {
  local status=$?
  trap - EXIT INT TERM
  if [[ -n "$server_pid" ]] && kill -0 "$server_pid" 2>/dev/null; then
    kill "$server_pid" 2>/dev/null || true
    wait "$server_pid" 2>/dev/null || true
  fi
  exit "$status"
}
trap cleanup EXIT INT TERM

ensure_s3tests

uv_bin="$(find_uv || true)"
if [[ -z "$uv_bin" ]]; then
  install_uv
  uv_bin="$(find_uv || true)"
fi

if [[ -z "$uv_bin" || ! -x "$uv_bin" ]]; then
  printf 'uv installation did not produce an executable uv binary\n' >&2
  exit 1
fi

rm -rf "$generated_dir"
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

write_s3ite_config
write_s3tests_config

if port_is_open; then
  printf 'port %s:%s is already in use; stop the existing service first\n' "$(connect_host)" "$s3ite_port" >&2
  exit 1
fi

if [[ -n "${S3ITE_BIN:-}" ]]; then
  s3ite_bin="$S3ITE_BIN"
else
  CARGO_TARGET_DIR="$cargo_target_dir" cargo build --release --manifest-path "$repo_root/Cargo.toml" --features binary --bin s3ite
  target_dir="$(
    CARGO_TARGET_DIR="$cargo_target_dir" cargo metadata --manifest-path "$repo_root/Cargo.toml" --no-deps --format-version 1 |
      "$venv_dir/bin/python" -c 'import json, sys; print(json.load(sys.stdin)["target_directory"])'
  )"
  s3ite_bin="$target_dir/release/s3ite"
fi

if [[ ! -x "$s3ite_bin" ]]; then
  printf 's3ite binary is not executable: %s\n' "$s3ite_bin" >&2
  exit 1
fi

printf 'Starting s3ite with %s\n' "$s3ite_conf"
"$s3ite_bin" --config "$s3ite_conf" >"$s3ite_log" 2>&1 &
server_pid=$!
wait_for_server

printf 'Running s3-tests with config %s\n' "$s3test_conf"
cd "$s3tests_dir"
S3TEST_CONF="$s3test_conf" "$venv_dir/bin/python" -m pytest "${pytest_args[@]}"
