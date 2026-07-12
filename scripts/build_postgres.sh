#!/bin/bash
#
# Build and install the vendored PostgreSQL (with Tiko patches) into
# target/pg-install. Run once on a fresh checkout; re-runnable.
#
# Usage: ./scripts/build_postgres.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
POSTGRES_DIR="${BASE_DIR}/postgres"
PG_INSTALL="${BASE_DIR}/target/pg-install"

# 1. Initialize the postgres git submodule if needed.
if [ ! -f "${POSTGRES_DIR}/configure" ]; then
    echo "Initializing postgres submodule..."
    git -C "${BASE_DIR}" submodule update --init postgres
fi

# 2. Install build dependencies.
OS="$(uname)"
if [ "${OS}" = "Darwin" ]; then
    if ! command -v brew &>/dev/null; then
        echo "ERROR: Homebrew is not installed. Install from https://brew.sh" >&2
        exit 1
    fi
    if ! xcode-select -p &>/dev/null; then
        echo "ERROR: Xcode Command Line Tools are not installed. Run: xcode-select --install" >&2
        exit 1
    fi
    echo "Installing build dependencies (macOS)..."
    brew install flex bison readline zlib pkg-config ccache
    # macOS ships outdated flex/bison; prefer Homebrew's.
    export PATH="$(brew --prefix bison)/bin:$(brew --prefix flex)/bin:${PATH}"
    export PKG_CONFIG_PATH="$(brew --prefix readline)/lib/pkgconfig:$(brew --prefix zlib)/lib/pkgconfig${PKG_CONFIG_PATH:+:${PKG_CONFIG_PATH}}"
elif [ "${OS}" = "Linux" ]; then
    echo "Installing build dependencies (Linux)..."
    sudo apt-get update
    sudo apt-get install -y \
        build-essential libreadline-dev zlib1g-dev flex bison \
        libxml2-dev libxslt-dev libssl-dev libxml2-utils xsltproc \
        ccache pkg-config
else
    echo "ERROR: Unsupported OS: ${OS}" >&2
    exit 1
fi

# 3. Configure.
echo "Configuring PostgreSQL..."
cd "${POSTGRES_DIR}"
./configure --prefix "${PG_INSTALL}" \
    --enable-debug \
    --enable-cassert \
    --without-openssl \
    --without-systemd \
    --without-libxml \
    --without-libxslt \
    --without-llvm \
    --without-icu \
    --without-selinux

# 4. Build Tiko smgr (staticlib linked into postgres at make time).
echo "Building Tiko smgr..."
if ! cargo build --manifest-path "${BASE_DIR}/Cargo.toml" -p smgr; then
    echo "ERROR: Tiko smgr build failed" >&2
    exit 1
fi
if [ ! -f "${BASE_DIR}/target/debug/libtikosmgr.a" ]; then
    echo "ERROR: Rust library libtikosmgr.a not found!" >&2
    exit 1
fi

# 5. Build and install.
echo "Building and installing PostgreSQL..."
if [ "${OS}" = "Darwin" ]; then
    JOBS="$(sysctl -n hw.ncpu)"
else
    JOBS="$(nproc)"
fi
make -j"${JOBS}" && make install

echo
echo "PostgreSQL installed to ${PG_INSTALL} 🎉"
echo
