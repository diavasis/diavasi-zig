#!/usr/bin/env bash
set -euo pipefail
version=$(tr -d '[:space:]' < VERSION)
tag="${GITHUB_REF_NAME:-}"
if [[ -n "$tag" && "$tag" != "v${version}" ]]; then
  echo "tag ${tag} does not match VERSION ${version}" >&2
  exit 1
fi
test -f c/include/diavasi.h
zig build
