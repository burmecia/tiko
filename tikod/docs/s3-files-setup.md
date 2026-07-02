# S3 Files mount inside the Firecracker VM

This is the runbook for mounting **AWS S3 Files** (`fs-02b6905b6653757b6`,
region `ap-southeast-2`, bucket `s3fs-test-3872`) into the Tiko Firecracker
guest VM at `/mnt/s3files` using the NFS protocol.

Everything below was verified working on 2026-07-01: `/mnt/s3files` mounts as
NFSv4.2 and supports read + write.

---

## TL;DR — how it works

S3 Files is built on EFS. It **always** uses TLS + IAM auth, so it cannot be
mounted with plain `mount -t nfs4`. Instead the `amazon-efs-utils` client
provides the `s3files` mount type, which starts a local `efs-proxy` (TLS +
IAM) and then exposes **NFSv4.2** to the guest:

```
guest app
   |  NFSv4.2 (localhost)
   v
efs-proxy (in guest) --TLS+IAM--> mount target 172.31.38.90:2049 --> S3 bucket
```

Data path from the guest: `eth0 (172.16.0.2)` -> host `tap0 (172.16.0.1)` ->
iptables MASQUERADE -> mount target. The host is EC2 `i-0c0a9f8f1605ed8af` in
`ap-southeast-2a`, **same subnet** (`subnet-070666d090e02d1e7`) as the mount
target `172.31.38.90` (AZ id `apse2-az3`), so there are no cross-AZ charges.

---

## One-time AWS prerequisites (already done — recorded for reference)

These persist in AWS, so they only need doing once.

### 1. Security group: allow NFS from the host

The mount target SG (`sg-03b5620eefe782099`, "default") only allowed inbound
from itself. VM traffic is SNAT'd to the host ENI, which carries the host SG
(`sg-06749e299d0eceba5`, "allow-all"), so we allow 2049 from that SG:

```bash
aws ec2 authorize-security-group-ingress \
  --group-id sg-03b5620eefe782099 \
  --ip-permissions '[{"IpProtocol":"tcp","FromPort":2049,"ToPort":2049,"UserIdGroupPairs":[{"GroupId":"sg-06749e299d0eceba5"}]}]' \
  --region ap-southeast-2
# rule id: sgr-0874c8e3013a75e47
```

### 2. IAM user for the guest (no IMDS in the guest)

The guest has **no instance metadata service**, so the mount helper cannot use
an instance profile. A least-privilege IAM user feeds it static credentials.

- User: `tiko-s3files-vm`
- Attached managed policy: `AmazonS3FilesClientFullAccess`
  (grants `s3files:ClientMount/ClientWrite/ClientRootAccess`).
- Inline policy `S3FilesVMBucketRead`: `s3:GetObject`/`GetObjectVersion` on
  `arn:aws:s3:::s3fs-test-3872/*`, `s3:ListBucket` on the bucket, and
  read-only control-plane on the file system (so the helper can call
  `s3files:GetMountTarget`/`ListMountTargets` even with `mounttargetip`).
- Access key id: `AKIAS7BNIVLMLNKO2KUS` (secret is in
  `tikod/assets/s3files-creds.env`, which is gitignored — never commit it).

To recreate the access key:
```bash
aws iam create-access-key --user-name tiko-s3files-vm --region ap-southeast-2
```

### 3. Mount target

Use `172.31.38.90` (same AZ as the host). All three AZ targets are reachable:
```bash
aws s3files list-mount-targets \
  --file-system-id fs-02b6905b6653757b6 --region ap-southeast-2
```

---

## What is baked into the rootfs

`tikod/scripts/create_rootfs.sh` now does the following when building the image
(so a freshly built VM auto-mounts on boot):

1. Installs `amazon-efs-utils` (>= 3.0.0) + `botocore` (already there).
2. Writes `/root/.aws/config` (`region = ap-southeast-2`).
3. Writes `/root/.aws/credentials` from either:
   - env vars `S3FILES_AWS_ACCESS_KEY_ID` / `S3FILES_AWS_SECRET_ACCESS_KEY`, or
   - `tikod/assets/s3files-creds.env` (gitignored), format:
     ```
     S3FILES_AWS_ACCESS_KEY_ID=AKIA...
     S3FILES_AWS_SECRET_ACCESS_KEY=...
     ```
   If neither is present, the creds file is left empty and the mount is skipped
   (with a warning) — the build still succeeds.
4. Adds the fstab entry so the mount comes up at boot (see below).
5. Copies `mount_s3files_vm.sh` into the guest as `/usr/local/sbin/mount-s3files`.
6. Enables `amazon-efs-mount-watchdog` for TLS-mount health monitoring.
7. Installs `s3files-postgres-owner.service`, a oneshot that runs after the
   mount comes up and chowns `/mnt/s3files` to `postgres:postgres` so the
   `postgres` user can write to it. The mounted root inode is normally owned by
   root; chowning it works because the guest IAM identity has `ClientRootAccess`,
   and the new ownership persists in S3 Files metadata across remounts (idempotent).
   It is skipped via `ConditionPathIsMountPoint` if the mount is absent (`nofail`).

fstab line added (env-overridable at build time via `S3FILES_FS_ID` /
`S3FILES_MOUNT_TARGET_IP`):
```
fs-02b6905b6653757b6:/ /mnt/s3files s3files _netdev,nofail,mounttargetip=172.31.38.90,tls,iam 0 0
```

So building with creds present:
```bash
S3FILES_AWS_ACCESS_KEY_ID=AKIA... S3FILES_AWS_SECRET_ACCESS_KEY=... \
  ./tikod/scripts/create_rootfs.sh
```

---

## Runtime

### Boot (automatic)
On a freshly built image the mount comes up automatically via fstab
(`_netdev` waits for networking; `nofail` means boot continues if unreachable).

### Manual mount (inside the VM)
```bash
mount -t s3files -o mounttargetip=172.31.38.90,tls,iam \
  fs-02b6905b6653757b6:/ /mnt/s3files
# or, if creds aren't baked: /usr/local/sbin/mount-s3files
```

### Unmount
```bash
umount /mnt/s3files
```

---

## Caveats (the gotchas that cost time)

1. **Credentials come from a file, not env vars.** This `efs-utils` build's
   botocore chain checks `/root/.aws/credentials`, `/root/.aws/config`, ECS
   URI, and IMDS — but **not** `AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY` env
   vars. Exporting them does nothing; write them to the credentials file.
2. **The endpoint is `.api.aws`, not `.amazonaws.com`.** botocore resolves
   `s3files.ap-southeast-2.api.aws` (public DNS, resolvable by `1.1.1.1`).
   `s3files.ap-southeast-2.amazonaws.com` does **not** resolve — don't use it
   for connectivity tests.
3. **No IMDS in the guest.** The guest is behind a NAT'd tap device; it cannot
   reach `169.254.169.254`. That's why we use a static IAM user, not an
   instance profile.
4. **Use the same-AZ mount target.** `172.31.38.90` is in the host's subnet.
   Picking another AZ still works but incurs cross-AZ data-transfer fees.
5. **Writes sync to S3 asynchronously.** A file written via the mount is
   readable immediately (read-after-write in the high-perf store) but is
   exported to S3 in the background (can be minutes). Sync config:
   `aws s3files get-synchronization-configuration --file-system-id
   fs-02b6905b6653757b6`. The import rule is scoped to prefix `tiko/`.
6. **`mounttargetip` bypasses DNS for the mount target itself**, but the helper
   still calls the control-plane API (`.api.aws`) to authorize the mount, so
   the guest still needs working DNS + outbound HTTPS.
7. **The IAM key is baked into the ext4 image.** The image is gitignored, but
   if you copy/share it, rotate the key (`aws iam create-access-key` /
   `delete-access-key` for `tiko-s3files-vm`).

---

## Verification (inside the VM, run from the host)

```bash
SSHC=(sshpass -p root ssh -o StrictHostKeyChecking=no \
       -o UserKnownHostsFile=/dev/null root@172.16.0.2)

"${SSHC[@]}" findmnt -T /mnt/s3files          # -> SOURCE 127.0.0.1:/  nfs4 vers=4.2
"${SSHC[@]}" df -h /mnt/s3files                # -> 8.0E capacity
"${SSHC[@]}" 'echo hi > /mnt/s3files/_t.txt && cat /mnt/s3files/_t.txt && rm /mnt/s3files/_t.txt'
"${SSHC[@]}" 'ps aux | grep -E "efs-proxy|efs-mount-watchdog" | grep -v grep'
```

---

## Optional: mount via the access point

There is an access point `fsap-0a2f291f3ae016ece` (root dir `/lambda`,
uid/gid 1000). To mount that instead of the whole bucket, add
`accesspoint=fsap-0a2f291f3ae016ece` to the mount options.
