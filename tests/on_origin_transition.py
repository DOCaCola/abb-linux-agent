"""Root-only driver regression on a newly created loop filesystem; never existing disks.

Run only in the disposable test VM. On timeout it preserves the module, loop,
mount and child PID for kernel-stack collection instead of waiting on cleanup.
"""
import argparse
import json
import multiprocessing as M
import os
from pathlib import Path
import subprocess
import tempfile
import time


from driver_control import check_abi, control


def control_child(pipe, operation, origin, cow):
    try:
        if operation in ('freeze','thaw'):
            subprocess.run(['fsfreeze','--freeze' if operation=='freeze' else '--unfreeze',str(cow)],
                check=True,capture_output=True)
            pipe.send({})
            return
        pipe.send(control(operation,origin,cow))
    except Exception as error:
        pipe.send(dict(failure=repr(error)))
    finally:
        pipe.close()


def writer(path, stop):
    with path.open('r+b',buffering=0) as stream:
        turn = 0
        while not stop.is_set():
            # Keep data and journal traffic active through freeze. Every write
            # stays in the explicitly created/preallocated specimen file.
            stream.seek((turn*97 % 8192)*4096)
            stream.write(bytes([turn%251])*4096)
            os.fdatasync(stream.fileno())
            turn += 1


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--module',required=True,type=Path)
    parser.add_argument('--filesystem',choices=('ext3','ext4','xfs'),required=True)
    parser.add_argument('--iterations',type=int,default=50)
    parser.add_argument('--external-freeze',action='store_true',
        help='also freeze/thaw the owned filesystem while its snapshot remains ACTIVE')
    parser.add_argument('--deadline',type=float,default=30)
    args = parser.parse_args()
    if os.geteuid()!=0 or Path('/sys/module/synosnap').exists():
        parser.error('requires root and an initially unloaded module in the disposable VM')
    check_abi()
    if args.iterations<1 or args.deadline<=0:
        parser.error('invalid bounds or unsupported ioctl ABI')
    root = Path(tempfile.mkdtemp(prefix='synosnap-on-origin-',dir='/var/tmp'))
    mount = root/'mount'
    mount.mkdir()
    image = root/'origin.img'
    with image.open('xb') as stream:
        stream.truncate(1024*1024*1024)
    image.chmod(0o600)
    module = str(args.module.resolve())
    loaded = mounted = owned = preserve = externally_frozen = False
    loop = None
    process = None
    stop = M.Event()
    results = []
    print(json.dumps(dict(artifacts=str(root),filesystem=args.filesystem)),flush=True)
    def run(*cmd):
        return subprocess.run(cmd,check=True,capture_output=True,text=True).stdout.strip()
    def ioctl(operation, cow):
        nonlocal preserve
        receive, send = M.Pipe(duplex=False)
        child = M.Process(target=control_child,args=(send,operation,loop,cow))
        child.start()
        send.close()
        if not receive.poll(args.deadline):
            preserve = True
            print(json.dumps(dict(timeout=operation,pid=child.pid,loop=loop,artifacts=str(root))),flush=True)
            # Do not join/kill a D-state ioctl child or touch its module/volume.
            os._exit(2)
        reply = receive.recv()
        child.join(2)
        receive.close()
        if 'failure' in reply:
            raise RuntimeError(reply['failure'])
        return reply
    try:
        if not run('modinfo','-F','vermagic',module).startswith(os.uname().release+' '):
            raise RuntimeError('module does not match the running kernel')
        run('mkfs.'+args.filesystem,'-f' if args.filesystem=='xfs' else '-F',str(image))
        run('insmod',module,'may_hook_syscalls=0','max_snap_devices=8')
        loaded = True
        run('udevadm','settle')
        loop = run('losetup','--find','--show',str(image))
        # The ext3 compatibility mount already uses immediate allocation and
        # rejects ext4's explicit nodelalloc option.
        options = 'nodev,nosuid'+(',delalloc' if args.filesystem=='ext4' else '')
        run('mount','-t',args.filesystem,'-o',options,loop,str(mount))
        mounted = True
        cow_dir = mount/'.synosnap_data'
        cow_dir.mkdir(mode=0o700)
        data = mount/'writer.data'
        with data.open('xb',buffering=0) as stream:
            for i in range(8192):
                stream.write(bytes([i%251])*4096)
            os.fsync(stream.fileno())
        run('sync','-f',str(mount))
        mounts = [line.split() for line in Path('/proc/mounts').read_text().splitlines()
                  if line.split()[1]==str(mount)]
        assert len(mounts)==1 and mounts[0][0]==loop and mounts[0][2]==args.filesystem
        options = mounts[0][3].split(',')
        if args.filesystem=='ext3':
            assert 'delalloc' not in options
        mounted_state = dict(device=loop,filesystem=mounts[0][2],options=options,
                             allocation='immediate (ext3)' if args.filesystem=='ext3'
                             else 'delalloc requested' if args.filesystem=='ext4' else 'XFS default')
        (root/'mount-state.json').write_text(json.dumps(mounted_state,indent=2))
        print(json.dumps(dict(mounted_state=mounted_state)),flush=True)
        for iteration in range(args.iterations):
            cow = cow_dir/f'cow-{iteration}'
            ioctl('setup',cow)
            owned = True
            before = ioctl('info',cow)
            assert before['state']&7==3 and before['error']==0
            stop.clear()
            process = M.Process(target=writer,args=(data,stop))
            process.start()
            time.sleep(.1)
            freeze_elapsed = None
            if args.external_freeze:
                freeze_start = time.monotonic()
                ioctl('freeze',mount)
                externally_frozen = True
                ioctl('thaw',mount)
                externally_frozen = False
                freeze_elapsed = time.monotonic()-freeze_start
            start = time.monotonic()
            ioctl('track',cow)
            elapsed = time.monotonic()-start
            after = ioctl('info',cow)
            assert after['state']&7==2 and after['error']==0 and not after['cancelled']
            assert after['sequence']==before['sequence'] and after['changed']>0
            stop.set()
            process.join(args.deadline)
            if process.is_alive():
                preserve = True
                print(json.dumps(dict(timeout='writer',pid=process.pid,loop=loop,artifacts=str(root))),flush=True)
                os._exit(2)
            assert process.exitcode==0
            process = None
            ioctl('destroy',cow)
            owned = False
            results.append(dict(iteration=iteration,seconds=elapsed,external_freeze_seconds=freeze_elapsed,
                                before=before,after=after))
            (root/'results.json').write_text(json.dumps(results,indent=2))
            print(json.dumps(results[-1]),flush=True)
    finally:
        stop.set()
        if not preserve:
            if externally_frozen:
                ioctl('thaw',mount)
            if process:
                process.join(args.deadline)
                if process.is_alive():
                    os._exit(2)
            if owned:
                ioctl('destroy',cow)
            if mounted:
                run('umount',str(mount))
            if loop:
                run('losetup','-d',loop)
            if loaded:
                run('rmmod','synosnap')


if __name__=='__main__':
    M.set_start_method('fork')
    main()
