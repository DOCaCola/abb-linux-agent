"""Root-only uncached snapshot oracle on an owned fresh ext4 loop filesystem.

Direct driver ioctls; no OpenSabba executable, configuration, or service state.
Timeouts preserve owned resources and child PIDs for kernel stack collection.
"""
import argparse
import fcntl
import hashlib
import json
import multiprocessing as M
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import time

from driver_control import check_abi, control


ORIGINAL = b'OpenSabba stable snapshot specimen\n'


def digest(path):
    result = hashlib.sha256()
    with path.open('rb',buffering=0) as source:
        while block := source.read(1024*1024):
            result.update(block)
    return result.hexdigest()


def control_child(pipe, operation, origin, cow):
    try:
        pipe.send(control(operation,origin,cow,cow_mib=64))
    except Exception as error:
        pipe.send(dict(failure=repr(error)))
    finally:
        pipe.close()


def writer(proof, stress_file, stop, deadline, writes):
    for index in range(64):
        if stop.is_set():
            return
        with proof.open('wb') as out:
            out.write(bytes([index])*1048576)
            out.flush()
            os.fsync(out.fileno())
        time.sleep(.02)
    with stress_file.open('r+b',buffering=0) as out:
        index = 0
        while not stop.is_set() and time.monotonic()<deadline:
            # Same odd stride and distinct replacement bytes as the e08cadf oracle.
            block = (index*2053)%8192
            out.seek(block*4096)
            out.write(bytes([252+index%4])*4096)
            writes.value += 1
            index += 1
            if index%16==0:
                os.fsync(out.fileno())
        os.fsync(out.fileno())


def reader(pipe, snapshot, baseline_file, root, deadline):
    try:
        baseline = baseline_file.read_bytes()
        reads = 0
        while reads<3 or time.monotonic()<deadline:
            with snapshot.open('rb',buffering=0) as source:
                # Invalidate this test-owned snapshot only, as in the original oracle.
                fcntl.ioctl(source.fileno(),0x1261)  # BLKFLSBUF
                chunks = []
                while block := source.read(1024*1024):
                    chunks.append(block)
            observed = b''.join(chunks)
            reads += 1
            result = dict(read=reads,sha256=hashlib.sha256(observed).hexdigest())
            if observed!=baseline:
                (root/'unstable.ext4').write_bytes(observed)
                result['failure'] = 'snapshot bytes changed during origin writes'
                result['changed_offsets'] = [at for at in range(0,len(baseline),4096)
                    if baseline[at:at+4096]!=observed[at:at+4096]][:32]
                pipe.send(result)
                return
            pipe.send(result)
        (root/'captured.ext4').write_bytes(observed)
        pipe.send(dict(done=True,reads=reads))
    except Exception as error:
        pipe.send(dict(failure=repr(error)))
    finally:
        pipe.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--module',required=True,type=Path)
    parser.add_argument('--stress-seconds',type=int,default=30)
    parser.add_argument('--deadline',type=float,default=30,
                        help='maximum inactivity for an ioctl/read/child shutdown')
    args = parser.parse_args()
    if os.geteuid()!=0 or Path('/sys/module/synosnap').exists():
        parser.error('requires root and an initially unloaded module in the disposable VM')
    check_abi()
    if not 1<=args.stress_seconds<=300 or args.deadline<=0:
        parser.error('stress duration must be 1..300 seconds and deadline must be positive')
    if shutil.disk_usage('/var/tmp').free<512*1024*1024:
        raise RuntimeError('requires at least 512 MiB free in /var/tmp')
    root = Path(tempfile.mkdtemp(prefix='synosnap-read-consistency-',dir='/var/tmp'))
    mount, image, cow = root/'mount',root/'origin.img',root/'snapshot.cow'
    mount.mkdir(mode=0o700)
    with image.open('xb') as out:
        out.truncate(64*1024*1024)
    image.chmod(0o600)
    module = str(args.module.resolve())
    loop = None
    loaded = mounted = owned = False
    process = read_process = None
    stop = M.Event()
    writes = M.Value('Q',0,lock=False)
    print(json.dumps(dict(artifacts=str(root))),flush=True)

    def run(*command):
        return subprocess.run(command,check=True,capture_output=True,text=True).stdout.strip()

    def timeout(operation, child):
        print(json.dumps(dict(timeout=operation,pid=child.pid,loop=loop,artifacts=str(root))),flush=True)
        # Do not join/kill D-state children or enter potentially blocked cleanup.
        os._exit(2)

    def ioctl(operation):
        receive, send = M.Pipe(duplex=False)
        child = M.Process(target=control_child,args=(send,operation,loop,cow))
        child.start()
        send.close()
        if not receive.poll(args.deadline):
            timeout(operation,child)
        reply = receive.recv()
        receive.close()
        child.join(args.deadline)
        if child.is_alive():
            timeout(operation,child)
        if 'failure' in reply:
            raise RuntimeError(reply['failure'])
        assert child.exitcode==0
        return reply

    def stop_writer():
        stop.set()
        if process:
            process.join(args.deadline)
            if process.is_alive():
                timeout('writer',process)

    try:
        if run('modinfo','-F','name',module)!='synosnap' or not run(
                'modinfo','-F','vermagic',module).startswith(os.uname().release+' '):
            raise RuntimeError('module does not match synosnap and the running kernel')
        run('mkfs.ext4','-F',str(image))
        run('insmod',module,'may_hook_syscalls=0','max_snap_devices=8')
        loaded = True
        run('udevadm','settle')
        assert Path('/sys/module/synosnap/parameters/may_hook_syscalls').read_text().strip()=='0'
        loop = run('losetup','--find','--show',str(image))
        run('mount','-t','ext4','-o','nodev,nosuid',loop,str(mount))
        mounted = True
        proof, stress_file = mount/'proof.txt',mount/'stress.bin'
        proof.write_bytes(ORIGINAL)
        with stress_file.open('xb',buffering=0) as out:
            for block in range(8192):
                out.write(bytes([block%251])*4096)
        run('sync','-f',str(mount))
        ioctl('setup')
        owned = True
        created = ioctl('info')
        assert created['state']&7==3 and created['error']==0 and not created['cancelled']
        run('udevadm','settle')
        snapshot = Path('/dev/synosnap0')
        baseline_file = root/'baseline.ext4'
        baseline = digest(snapshot)
        with snapshot.open('rb',buffering=0) as source, baseline_file.open('xb') as target:
            shutil.copyfileobj(source,target)
        assert digest(baseline_file)==baseline
        deadline = time.monotonic()+args.stress_seconds
        process = M.Process(target=writer,args=(proof,stress_file,stop,deadline,writes))
        process.start()
        receive, send = M.Pipe(duplex=False)
        read_process = M.Process(target=reader,args=(send,snapshot,baseline_file,root,deadline))
        read_process.start()
        send.close()
        with (root/'reads.jsonl').open('w') as log:
            while True:
                if not receive.poll(args.deadline):
                    timeout('snapshot-read',read_process)
                result = receive.recv()
                log.write(json.dumps(result)+'\n')
                log.flush()
                if 'failure' in result or result.get('done'):
                    break
        receive.close()
        read_process.join(args.deadline)
        if read_process.is_alive():
            timeout('snapshot-reader',read_process)
        assert read_process.exitcode==0
        stop_writer()
        assert process.exitcode==0
        state = ioctl('info')
        (root/'snapshot-state.json').write_text(json.dumps(state,indent=2))
        if 'failure' in result:
            shutil.copyfile(cow,root/'unstable.cow')
            raise AssertionError(result)
        assert result['reads']>=3 and writes.value>0
        assert digest(root/'captured.ext4')==baseline and digest(Path(loop))!=baseline
        assert run('debugfs','-R','cat /proof.txt',str(root/'captured.ext4'))==ORIGINAL.decode().strip()
        assert state['state']&7==3 and state['error']==0 and not state['cancelled']
        assert state['changed']>0 and state['uuid']==created['uuid']
        summary = dict(passed=True,reads=result['reads'],scattered_writes=writes.value,
            stress_seconds=args.stress_seconds,baseline_sha256=baseline,state=state)
        (root/'results.json').write_text(json.dumps(summary,indent=2))
        print(json.dumps(summary),flush=True)
    finally:
        stop_writer()
        if read_process and read_process.is_alive():
            timeout('snapshot-reader',read_process)
        if owned:
            ioctl('destroy')
        if mounted:
            run('umount',str(mount))
        if loop:
            run('losetup','-d',loop)
        if loaded:
            run('rmmod','synosnap')
        print('Owned snapshot, mount, loop device and module cleaned up',flush=True)


if __name__=='__main__':
    M.set_start_method('fork')
    main()
