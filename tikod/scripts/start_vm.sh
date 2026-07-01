#!/bin/bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ASSETS_DIR="$SCRIPT_DIR/../assets"
FC_DIR="$SCRIPT_DIR/../../firecracker/build/cargo_target/x86_64-unknown-linux-musl/debug"
API_SOCKET=/tmp/firecracker.socket

sudo rm -f $API_SOCKET
sudo $FC_DIR/firecracker --api-sock $API_SOCKET --config-file $ASSETS_DIR/vm_config.json
