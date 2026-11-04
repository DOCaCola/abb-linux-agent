"""Linux 64-bit synosnap ioctl ABI from build-tools/patches/synosnap/elastio-snap.h."""
import ctypes as C
import os


class Setup(C.Structure):
    _fields_ = [('bdev',C.c_char_p),('cow',C.c_char_p),('mib',C.c_uint64),
                ('cache',C.c_uint64),('minor',C.c_uint),('ignore',C.c_bool)]


class Info(C.Structure):
    _fields_ = [('cancelled',C.c_bool),('minor',C.c_uint),('flags',C.c_uint),
        ('state',C.c_uint64),('error',C.c_int),('cache',C.c_uint64),('allocation',C.c_ulonglong),
        ('sequence',C.c_ulonglong),('uuid',C.c_ubyte*16),('cow',C.c_char*4096),
        ('bdev',C.c_char*4096),('version',C.c_ulonglong),('changed',C.c_ulonglong),('ignore',C.c_bool)]


def request(direction, number, structure):
    return direction << 30 | C.sizeof(structure) << 16 | ord('A') << 8 | number


REQUESTS = dict(setup=request(1,1,Setup), info=request(2,8,Info),
                track=request(1,5,C.c_uint), destroy=request(1,4,C.c_uint))


def check_abi():
    if C.sizeof(C.c_void_p)!=8 or C.sizeof(Setup)!=40 or C.sizeof(Info)!=8288:
        raise RuntimeError('requires the Linux 64-bit synosnap ioctl ABI')


def control(operation, origin, cow, minor=0, cow_mib=128):
    check_abi()
    libc = C.CDLL(None,use_errno=True)
    fd = os.open('/dev/synosnap-ctl',os.O_RDONLY)
    try:
        argument = (Setup(origin.encode(),str(cow).encode(),cow_mib,0,minor,False)
                    if operation=='setup' else Info(minor=minor) if operation=='info' else C.c_uint(minor))
        if libc.ioctl(fd,C.c_ulong(REQUESTS[operation]),C.byref(argument)):
            raise OSError(C.get_errno(),operation)
        if operation=='info':
            return dict(state=argument.state,error=argument.error,sequence=argument.sequence,
                cancelled=argument.cancelled,changed=argument.changed,uuid=bytes(argument.uuid).hex(),
                cow=argument.cow.decode(),bdev=argument.bdev.decode())
        return {}
    finally:
        os.close(fd)
