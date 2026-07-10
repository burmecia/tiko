#!/bin/bash
#
# Mount the AWS S3 Files file system into the Firecracker guest VM over NFSv4.2.
#
# S3 Files always uses TLS + IAM auth via the amazon-efs-utils mount helper
# (the `s3files` mount type), which runs a local efs-proxy and exposes NFSv4.2
# to the guest. Run this script INSIDE the VM.
#
# Network path: guest eth0 (172.16.0.0/24) -> host tap0 -> MASQUERADE ->
# mount target 172.31.38.90:2049 (same AZ as the host, ap-southeast-2a).
#
# IAM credentials: the VM has no instance metadata, so feed the mount helper
# via env vars or /root/.aws/credentials. The least-privilege IAM user is
# `tiko-s3files-vm` (ClientMount/ClientWrite/ClientRootAccess + bucket read).
#
# Usage:
#   # one-shot mount (creds from env):
#   AWS_ACCESS_KEY_ID=... AWS_SECRET_ACCESS_KEY=... ./mount_s3files_vm.sh
#   # or from /root/.aws/credentials (default), then:
#   ./mount_s3files_vm.sh
#   # make it auto-mount on boot:
#   ./mount_s3files_vm.sh --persist

set -euo pipefail

FILE_SYSTEM_ID=fs-02b6905b6653757b6
MOUNT_TARGET_IP=172.31.38.90        # apse2-az3, same subnet as the host
AWS_REGION=ap-southeast-2
MOUNT_POINT=/mnt/s3files

# Optional access point (root dir "/lambda", uid/gid 1000). Leave empty to mount
# the whole bucket. Uncomment to use it.
# ACCESS_POINT=fsap-0a2f291f3ae016ece

PERSIST=false
if [[ "${1:-}" == "--persist" ]]; then
    PERSIST=true
fi

if [[ $EUID -ne 0 ]]; then
    echo "must run as root" >&2
    exit 1
fi

command -v mount.s3files >/dev/null 2>&1 || {
    echo "mount.s3files not found - install amazon-efs-utils (>= 3.0.0)" >&2
    exit 1
}

mkdir -p "$MOUNT_POINT"

# The mount helper's botocore chain prefers env vars, then ~/.aws/credentials,
# then IMDS (unavailable in the guest). Export for the helper subprocess.
export AWS_DEFAULT_REGION="${AWS_DEFAULT_REGION:-$AWS_REGION}"
export AWS_REGION="${AWS_REGION:-$AWS_DEFAULT_REGION}"

OPTS="mounttargetip=${MOUNT_TARGET_IP},tls,iam"
if [[ -n "${ACCESS_POINT:-}" ]]; then
    OPTS="${OPTS},accesspoint=${ACCESS_POINT}"
fi

if ! findmnt -T "$MOUNT_POINT" >/dev/null 2>&1; then
    echo "mounting $FILE_SYSTEM_ID at $MOUNT_POINT ..."
    mount -t s3files -o "$OPTS" "${FILE_SYSTEM_ID}:/" "$MOUNT_POINT"
else
    echo "$MOUNT_POINT already mounted"
fi

echo "mounted:"
findmnt -T "$MOUNT_POINT"

if $PERSIST; then
    # Boot-time mount needs creds available before networking+mount; write a
    # credentials file the helper reads (env vars are not set during boot).
    mkdir -p /root/.aws
    if [[ -n "${AWS_ACCESS_KEY_ID:-}" && -n "${AWS_SECRET_ACCESS_KEY:-}" ]]; then
        cat > /root/.aws/credentials <<EOF
[default]
aws_access_key_id = ${AWS_ACCESS_KEY_ID}
aws_secret_access_key = ${AWS_SECRET_ACCESS_KEY}
EOF
        chmod 600 /root/.aws/credentials
    fi

    FSTAB_LINE="${FILE_SYSTEM_ID}:/ ${MOUNT_POINT} s3files _netdev,nofail,${OPTS} 0 0"
    if ! grep -q "^${FILE_SYSTEM_ID}:/ ${MOUNT_POINT} " /etc/fstab 2>/dev/null; then
        printf '%s\n' "$FSTAB_LINE" >> /etc/fstab
        echo "added fstab entry:"
        grep "$FILE_SYSTEM_ID" /etc/fstab
    else
        echo "fstab entry already present"
    fi
fi
