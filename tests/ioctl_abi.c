/* Compile for Linux x86_64/aarch64 to check the ctypes binding against the exported ABI. */
#include <stddef.h>
#include "../build-tools/patches/synosnap/elastio-snap.h"

_Static_assert(sizeof(struct setup_params)==40, "Setup size");
_Static_assert(offsetof(struct setup_params,minor)==32, "Setup minor");
_Static_assert(offsetof(struct setup_params,ignore_snap_errors)==36, "Setup ignore");
_Static_assert(sizeof(struct elastio_snap_info)==8288, "Info size");
_Static_assert(offsetof(struct elastio_snap_info,state)==16, "Info state");
_Static_assert(offsetof(struct elastio_snap_info,error)==24, "Info error");
_Static_assert(offsetof(struct elastio_snap_info,seqid)==48, "Info sequence");
_Static_assert(offsetof(struct elastio_snap_info,uuid)==56, "Info uuid");
_Static_assert(offsetof(struct elastio_snap_info,cow)==72, "Info cow");
_Static_assert(offsetof(struct elastio_snap_info,bdev)==4168, "Info origin");
_Static_assert(offsetof(struct elastio_snap_info,nr_changed_blocks)==8272, "Info changed blocks");
_Static_assert(IOCTL_SETUP_SNAP==0x40284101UL, "setup ioctl");
_Static_assert(IOCTL_DESTROY==0x40044104UL, "destroy ioctl");
_Static_assert(IOCTL_TRANSITION_INC==0x40044105UL, "tracking ioctl");
_Static_assert(IOCTL_ELASTIO_SNAP_INFO==0xa0604108UL, "info ioctl");
