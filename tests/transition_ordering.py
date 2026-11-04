"""Compile/run the actual transition and worker functions with a deterministic kernel shim.

No module/device access. This checks park/drain/publication/error ordering; it
cannot replace the live ext3 journal and snapshot-integrity recipe.
"""
import argparse
from pathlib import Path
import re
import subprocess
import tempfile


def function(source, name):
    match = re.search(r'^static[^\n]*\b'+name+r'\([^\n]*\)\s*\{',source,re.M)
    start = match.start()
    at = source.index('{',start)+1
    depth = 1
    while depth:
        depth += (source[at]=='{')-(source[at]=='}')
        at += 1
    return source[start:at]


PRELUDE = r'''
#include <assert.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>
#include <setjmp.h>
#include <stdio.h>
#undef assert
#define assert(condition) do {if(!(condition)){fprintf(stderr,"ordering assertion: %s",#condition);fputc(10,stderr);exit(99);}} while(0)
#define ACTIVE 1
#define MIN_NICE 0
#define HAVE_BIO_BI_POOL 1
#define LOG_DEBUG(...) ((void)0)
#define LOG_ERROR(...) ((void)0)
#define LOG_WARN(...) ((void)0)
#define current NULL
#define spin_lock_irqsave(lock,flags) ((void)(lock),(flags)=0)
#define spin_unlock_irqrestore(lock,flags) ((void)(lock),(void)(flags))
#define wait_event_interruptible(event,condition) assert(condition)
#define atomic_set(ptr,n) (*(ptr)=(n))
#define atomic64_read(ptr) (*(ptr))
#define atomic64_inc(ptr) (++*(ptr))
#define clear_bit(bit,ptr) (*(ptr)&=~(1UL<<(bit)))
#define set_bit(bit,ptr) (*(ptr)|=1UL<<(bit))
#define test_bit(bit,ptr) (!!(*(ptr)&(1UL<<(bit))))
#define bio_data_dir(bio) ((bio)->write)
#define bio_list_for_each(bio,list) for((bio)=(list)->head;(bio);(bio)=(bio)->bi_next)
#define kfree(ptr) fake_free(ptr)
#define SNAPSHOT 0
#define UNVERIFIED 2
struct bio;
struct snap_device;
struct bio_sector_map {struct bio *bio; bool orig_released; struct bio_sector_map *next;};
struct tracing_params {struct snap_device *dev; struct {struct bio_sector_map *head;} bio_sects;};
struct bio {struct tracing_params *bi_private; struct bio *bi_next; bool write;};
struct bio_list {struct bio *head;};
struct bio_queue {int lock,event; struct bio_list bios;};
struct task_struct {bool parked;};
struct cow_manager {int filp; struct snap_device *dev;};
struct request_queue {int unused;};
struct snap_device {
 unsigned long sd_state; unsigned sd_minor; int sd_ignore_requests,sd_ignore_snap_errors;
 long sd_submitted_cnt,sd_received_cnt,sd_processed_cnt;
 struct bio_queue sd_cow_bios; struct task_struct *sd_cow_thread;
 struct cow_manager *sd_cow; struct request_queue *sd_queue;
};
static struct snap_device old;
static struct task_struct old_task,new_task;
static struct cow_manager cow;
static struct request_queue queue;
static struct bio read_bio,write_bio;
static struct tracing_params tp;
static struct bio_sector_map map;
static bool park_request,stop_request,frozen,published,old_drained,new_started;
static unsigned reads,writes,released,references;
static int mode;
static jmp_buf parked;
static int snap_cow_thread(void *data);
static int bio_queue_empty(const struct bio_queue *q){return !q->bios.head;}
static bool kthread_should_stop(void){return stop_request;}
static bool kthread_should_park(void){return park_request;}
static void kthread_parkme(void){old_task.parked=true;longjmp(parked,1);}
static void set_user_nice(void *task,int n){(void)task;(void)n;}
static struct bio *bio_queue_dequeue_delay_read(struct bio_queue *q){
 struct bio *b=q->bios.head;q->bios.head=b->bi_next;b->bi_next=NULL;return b;
}
static void tp_release_orig(struct tracing_params *p){(void)p;++released;}
static void tp_release(struct tracing_params *p){(void)p;++references;}
static void tp_put(struct tracing_params *p){tp_release_orig(p);tp_release(p);}
static void bio_free_pages(struct bio *b){(void)b;}
static void bio_put(struct bio *b){(void)b;}
static void cow_free_members(struct cow_manager *c){(void)c;}
static int snap_is_cancelled(struct cow_manager *c){(void)c;return 0;}
static int wrap_err_io(struct snap_device *d){(void)d;return -5;}
static int tracer_read_fail_state(struct snap_device *d){return mode==3&&published&&d==&old?-5:0;}
static void tracer_set_fail_state(struct snap_device *d,int err){(void)d;assert(err==-5);}
static int snap_handle_read_bio(struct snap_device *d,struct bio *b){
 (void)d;(void)b;assert(!frozen);++reads;return 0;
}
static int snap_handle_write_bio(struct snap_device *d,struct bio *b){
 (void)d;(void)b;assert(!frozen);++writes;return 0;
}
static void elastio_snap_bio_endio(struct bio *b,int err){(void)b;assert(!err);}
static void blk_mq_start_stopped_hw_queues(struct request_queue *q,bool async){(void)q;(void)async;}
static void fake_free(void *d){if(d!=&old) free(d);}
static int tracer_alloc(struct snap_device **d){*d=calloc(1,sizeof(**d));return *d?0:-12;}
static void __tracer_copy_base_dev(struct snap_device *a,struct snap_device *b){b->sd_minor=a->sd_minor;}
static void __tracer_copy_cow_path(struct snap_device *a,struct snap_device *b){(void)a;(void)b;}
static void __tracer_copy_cow(struct snap_device *a,struct snap_device *b){b->sd_cow=a->sd_cow;b->sd_cow->dev=b;}
static int __tracer_setup_inc_cow_thread(struct snap_device *d,unsigned minor){
 (void)minor;d->sd_cow_thread=&new_task;new_task.parked=true;return 0;
}
static void __tracer_stop_cow_readers(struct snap_device *d){d->sd_ignore_requests=1;}
static int kthread_park(struct task_struct *task){
 assert(task==&old_task&&old.sd_ignore_requests&&!frozen);
 if(mode==2)return -5;
 park_request=true;
 if(!setjmp(parked)){snap_cow_thread(&old);assert(!"worker did not park");}
 assert(reads==1&&writes==0&&old.sd_cow_bios.bios.head==&write_bio);
 return 0;
}
static void kthread_unpark(struct task_struct *task){
 assert(task==&old_task&&task->parked&&!frozen);task->parked=false;park_request=false;
}
static int __tracer_setup_tracing(struct snap_device *d,unsigned minor){
 (void)d;(void)minor;
 assert(old_task.parked&&old.sd_ignore_requests&&!test_bit(ACTIVE,&old.sd_state));
 assert(map.orig_released&&released==1&&!new_started);
 frozen=true;
 /* The journal can finish: no old COW worker can join it and no buffered
    origin write waits on that worker. */
 assert(old_task.parked&&released==1);
 frozen=false;
 if(mode==1)return -22;
 published=true;return 0;
}
static void __tracer_destroy_cow_thread(struct snap_device *d){
 if(d==&old){
  assert(published&&!old_task.parked&&!new_started);
  stop_request=true;snap_cow_thread(d);old_drained=true;
 }else assert(!new_started);
}
static void wake_up_process(struct task_struct *task){
 assert(task==&new_task&&old_drained&&writes==1&&references==1&&released==1);
 new_started=true;
}
static void __tracer_destroy_snap(struct snap_device *d){assert(d==&old&&old_drained);}
static int cow_truncate_to_index(struct cow_manager *c){(void)c;assert(new_started);return mode==4?-5:0;}
static void file_get_absolute_pathname(int f,char **path,int *n){(void)f;*path=NULL;*n=0;}
'''

MAIN = r'''
int main(void){
 (void)__tracer_release_buffered_origins;(void)__tracer_start_cow_readers;
 (void)kthread_park;(void)kthread_unpark;(void)__tracer_stop_cow_readers;
 for(mode=0;mode<6;++mode){
  old=(struct snap_device){0};old_task=(struct task_struct){0};new_task=(struct task_struct){0};
  read_bio=(struct bio){0};write_bio=(struct bio){0};map=(struct bio_sector_map){0};
  park_request=stop_request=frozen=published=old_drained=new_started=false;
  reads=writes=released=references=0;
  set_bit(SNAPSHOT,&old.sd_state);set_bit(ACTIVE,&old.sd_state);
  cow.dev=&old;old.sd_minor=5;old.sd_cow_thread=&old_task;old.sd_cow=&cow;old.sd_queue=&queue;
  read_bio.bi_next=&write_bio;write_bio.write=true;write_bio.bi_private=&tp;
  tp.dev=&old;tp.bio_sects.head=&map;map.bio=&write_bio;
  if(mode==5){map.orig_released=true;released=1;}
  old.sd_cow_bios.bios.head=&read_bio;
  int ret=tracer_active_snap_to_inc(&old);
  assert(!frozen&&!old_task.parked);
  if(mode==1||mode==2){
   assert(ret==(mode==1?-22:-5)&&!published&&!new_started);
   assert(test_bit(ACTIVE,&old.sd_state)&&!old.sd_ignore_requests&&cow.dev==&old);
   if(mode==1){
    write_bio.bi_next=&read_bio;stop_request=true;snap_cow_thread(&old);
    assert(reads==2&&writes==1&&released==1&&references==1);
   }
  }else{
   assert(ret==(mode==3?-5:0)&&published&&new_started&&old_drained);
  }
 }
 puts("6 actual-source transition/park/drain/error/refcount cases passed");
 return 0;
}
'''


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--cc',default='cc')
    parser.add_argument('--control-revision',help='require the historical transition to fail the freeze-safety oracle')
    args = parser.parse_args()
    source = (Path(__file__).resolve().parents[1]/'build-tools/patches/synosnap/main.c').read_text(encoding='utf-8')
    functions = ('bio_queue_has_read','bio_free_clone','snap_cow_thread',
        '__tracer_release_buffered_origins','__tracer_start_cow_readers','tracer_active_snap_to_inc')
    historical = source
    if args.control_revision:
        historical = subprocess.check_output(['git','-C',str(Path(__file__).resolve().parents[1]),
            'show',args.control_revision+':build-tools/patches/synosnap/main.c']).decode('utf-8')
    with tempfile.TemporaryDirectory(prefix='synosnap-transition-') as temporary:
        root = Path(temporary)
        c, exe = root/'transition.c', root/'transition-test'
        text = PRELUDE.replace('mode==3&&published&&d==&old','mode==3&&old_drained&&d==&old')
        c.write_text(text+'\n'+'\n'.join(function(historical if name=='tracer_active_snap_to_inc' else source,name) for name in functions)+'\n'+MAIN,encoding='utf-8')
        subprocess.run([args.cc,'-std=c11','-Wall','-Wextra','-Werror',str(c),'-o',str(exe)],check=True)
        result = subprocess.run([str(exe)],capture_output=bool(args.control_revision),text=True)
        if args.control_revision:
            assert result.returncode==99 and 'old_task.parked' in result.stderr,result
            print('Historical transition rejected by the freeze-safety oracle as expected')
        else:
            result.check_returncode()


if __name__=='__main__':
    main()
