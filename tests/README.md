# Snapshot-to-tracking freeze regression

Snapshot COW workers are parked before sync/freeze and only resumed after
thaw, including the new worker at initial snapshot publication and renewal. Snapshot readers have drained first; late cloned old bytes remain buffered
and their original writes are released with the existing dormant policy. The
old worker then drains before the new incremental worker starts. Active snapshot
reads retain the e08cadf ordering. File-backed writes use stock kernel_write
writer admission; the global __kernel_write bypass is removed. No user-space
freeze or retry is needed.

## Offline ordering check

From this repository root:

```sh
python3 tests/transition_ordering.py --cc cc
python3 tests/transition_ordering.py --cc cc --control-revision 78520e8
python3 tests/transition_ordering.py --cc cc --publication-control-revision 99a5e13
python3 tests/transition_ordering.py --cc cc --writer-control-revision 99a5e13
```

This compiles the actual source functions with `-std=c11 -Wall -Wextra -Werror`
and deterministic kernel interposition. Nine cases cover reader drain before
park, no old COW writer during freeze, old/new manager serialization, rejected
park, rejected freeze with restored snapshot/manager ownership, failed old COW
cleanup, truncation warning, already-released clone refcounts, initial/renewed
snapshot publication, tracing removal, and normal writer admission through the
entire thaw callback. The actual common tracing transition and COW write wrapper
are compiled, rather than replacing the complete freeze/thaw path with a stub.
The shim represents waiting at writer admission with a sentinel return; it does
not predict a real kernel_write error. This is a
control-flow/refcount test, not a kernel/journal or snapshot-content proof.
All three historical-control commands must reject their old implementation: the
78520e8 retiring-worker path, 99a5e13 new-snapshot publication and the 693820e
writer-admission bypass.

## Build on the exact test kernel

Use a clean disposable VM, with its running kernel's matching headers installed.
Do not install the module or unload a driver serving existing snapshots.
Export sources with LF line endings from the selected commit if transferring
from Windows:

```sh
git -c core.autocrlf=false -c core.eol=lf archive --format=tar.gz \
  -o /tmp/synosnap-transition-source.tar.gz HEAD build-tools/patches/synosnap tests
mkdir /var/tmp/synosnap-transition-build
tar -xf /tmp/synosnap-transition-source.tar.gz -C /var/tmp/synosnap-transition-build
cd /var/tmp/synosnap-transition-build
sudo make -C build-tools/patches/synosnap KERNELVERSION="$(uname -r)" NETLINK_DEBUG=n -j4
modinfo -F vermagic build-tools/patches/synosnap/synosnap.ko
```

The Windows/offline workspace has no matching kernel build headers or generated
kernel configuration, so a complete module build must run here before testing.
Keep build output, source commit, `uname -a`, module SHA-256 and `modinfo`.

## Repeated on-origin transition

The test requires root and an initially unloaded synosnap module. It creates its
own 1 GiB loop image, mounts only that image, puts the COW under its own mounted
`.synosnap_data`, and keeps a writer/fsync workload active across setup→tracking.
Every repetition is a fresh snapshot, followed by a healthy index-only transition
and destruction; it does not seed driver history or touch NAS/service state.

```sh
module="$PWD/build-tools/patches/synosnap/synosnap.ko"
sudo python3 tests/on_origin_transition.py --module "$module" \
  --filesystem ext3 --iterations 50 --deadline 30
sudo python3 tests/on_origin_transition.py --module "$module" \
  --filesystem ext4 --iterations 50 --deadline 30
sudo python3 tests/on_origin_transition.py --module "$module" \
  --filesystem xfs --iterations 50 --deadline 30
```

Ext3 uses `-t ext3` with default allocation options: it already allocates without
delay and rejects an explicit `nodelalloc` option. The script confirms the
`ext3` filesystem type from `/proc/mounts` and records its options in
`mount-state.json`. Ext4 explicitly requests `delalloc`; XFS uses its defaults.
The script checks snapshot state 3→tracking state 2, unchanged generation,
nonzero changed-block count and error/cancel state. Each ioctl runs in a bounded
child. If it times out, the script exits without joining a D-state child or
attempting blocked driver cleanup. It prints the owned loop/mount/artifact path
and PID. Preserve `/proc/PID/stack`, old/new COW thread stacks and kernel logs;
avoid `/proc/synosnap` or new control ioctls while the ioctl mutex is held.
Successful runs clean up only their own snapshot, mount, loop and module and
retain `results.json` with all transition durations.

Require all 50 repetitions on each filesystem, no D-state transition/writer,
no new filesystem/driver warning or error, and ordinary unmount/module unload.
A transition completing with ext4_journal_check_start warnings still fails the
gate; 99a5e13 completed 50 ext3 repetitions but produced one warning per cycle. A single
successful transition is insufficient for the reported intermittent deadlock.

## Retain the original snapshot-read proof

Separately, from the OpenSabba checkout, use its current native lab executable
and the same compiled module. This test makes its own disposable ext4 loop
volume and external COW, disables syscall hooks, and checks uncached snapshot
reads against immutable bytes while scattered writes continue:

```sh
for repetition in 1 2 3 4 5; do
  sudo python3 tests/test_snapshot_live.py /absolute/path/opensabba-lab \
    --module /absolute/path/synosnap.ko --stress-seconds 30 --transitions
done
```

All uncached reads must remain byte-exact, with healthy snapshot state; renewals
and tracking must complete. Retain its immutable baseline, read hashes, changed
block records and logs. This is the scenario fixed by e08cadf and must not be
traded for successful transitions. Finally rerun the frozen batch-O C6 specimen
with the separately approved tester workflow and confirm the original ext3
origin reaches tracking after upload, then perform normal successor/readback
checks. No existing source volume belongs to these standalone scripts.
