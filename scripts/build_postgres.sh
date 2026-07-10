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
echo "Installing build dependencies..."
sudo apt-get update
sudo apt-get install -y \
    build-essential libreadline-dev zlib1g-dev flex bison \
    libxml2-dev libxslt-dev libssl-dev libxml2-utils xsltproc \
    ccache pkg-config

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

# 4. Build and install.
echo "Building and installing PostgreSQL..."
make -j"$(nproc)" && make install

echo "PostgreSQL installed to ${PG_INSTALL}"
