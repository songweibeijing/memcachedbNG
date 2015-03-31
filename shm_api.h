/*
 * share memory library,
 */

#ifndef _SHM_API_H
#define _SHM_API_H

#include <asm/types.h>
#include <dirent.h>
#include <stdlib.h>
#include <unistd.h>
#include <asm/unistd.h>
#include <string.h>
#include <utime.h>
#include <errno.h>
#include <pthread.h>
#include <stdio.h>
#include <sys/types.h>
#include <sys/mman.h>
#include <fcntl.h>
#include <sys/file.h>
#include <assert.h>
#include "linux_list.h"

#if defined(__cplusplus)
extern "C" {
#endif

    /*
     * share memory attribute, entry number and size of each entry.
     * each entry has same size.
     */
    struct shm_attr
    {
        /* meta data before data array */
        size_t          meta_size;
        unsigned            n_entry;
        size_t          entry_size;
    };

    /* alignment of share memory object */
#define SHM_ALIGN       __alignof__(__u64)

    /* shm object size is aligned */
#define SHM_SIZE(x)     (((x)+ SHM_ALIGN -1)&~(SHM_ALIGN - 1))

#define SHM_NAME_SIZE       256

    /* who hold the write lock */
    struct shm_wlocker
    {
        pid_t           pid;
        char            func[128];
        int             line;
    };

    /* descriptor of shm, header of mmaped shm files */
    struct shm_descriptor
    {
        /* shm age */
        int                 age;
        /* shm attr */
        struct shm_attr     attr;
        struct shm_wlocker  wlocker;
        /* shm objects array */
        char                buf[0]  __attribute__((aligned(SHM_ALIGN)));
    };

    /* share memory handle */
    struct shm_handle
    {
        /* shm name */
        char                            name[SHM_NAME_SIZE];
        /* shm fd */
        int                         fd;
        /* rwlock */
        pthread_rwlock_t                rwlock;
        /* mmaped pointer */
        struct shm_descriptor           *desc;
    };

    /*
     * Note: shm_handle_t can not be inherited from parent,
     * because rwlock will not synchronize between parent/child processes.
     *
     * So, after fork, please shm_detach/shm_create again.
     */
    typedef struct shm_handle           *shm_handle_t;

#define SHM_HANDLE_FAIL     ((struct shm_handle *)NULL)

    /* read/write lock of share memory handle */
    void shm_rlock(shm_handle_t handle) __attribute__((nonnull));
    void shm_runlock(shm_handle_t handle) __attribute__((nonnull));

    void __shm_wlock(shm_handle_t handle, pid_t pid, const char *func, int line)
    __attribute__((nonnull));
    void __shm_wunlock(shm_handle_t handle) __attribute__((nonnull));

#define shm_wlock(handle)   __shm_wlock(handle, getpid(), __FUNCTION__, __LINE__)

#define shm_wunlock(handle) __shm_wunlock(handle)

    /*
     * Return value: -1 if failed,
     *  otherwise success.
     */
    int shm_get_wlocker(const char *name, struct shm_wlocker *wlocker);

    /*
     * create shm,
     * will try to attach shm if shm already exists(config daemon restarts)
     * @name: name of shm
     * @attr: attribute of shm
     *
     * Return value: SHM_HANDLE_FAIL if failed,
     *  otherwise success.
     */
    shm_handle_t shm_create(char *name, struct shm_attr *attr) __attribute__((nonnull));

    /* destroy shm */
    void shm_destroy(char *name) __attribute__((nonnull));;

    /*
     * attach shm, the shm should be created before.
     * @name: name of shm
     *
     * Return value: SHM_HANDLE_FAIL if failed,
     *  otherwise successful
     */
    shm_handle_t shm_attach(char *name) __attribute__((nonnull));

    /* detach shm handle */
    void shm_detach(shm_handle_t handle) __attribute__((nonnull));

    /*
     * get attr of shm
     * @handle: input, shm handle
     * @attr: output, shm attr
     */
    static inline void __attribute__((nonnull))
    shm_get_attr(shm_handle_t handle, struct shm_attr *attr)
    {
        assert(handle && handle->desc && attr);
        *attr = handle->desc->attr;
    }

    /*
     * get meta pointer in shm,
     * @handle: shm handle
     *
     * Return value: NULL if no meta data,
     *  otherwise successful
     */
    void *shm_meta_ptr(shm_handle_t handle)
    __attribute__((warn_unused_result)) __attribute__((nonnull));


    /*
     * get object pointer in shm,
     * normally used in writing to shm while holding shm_wlock, or
     * reading atomic shm.
     * @handle: shm handle
     * @index: index of shm entry
     *
     * Return value: NULL if failed,
     *  otherwise successful
     */
    void *shm_obj_ptr(shm_handle_t handle,  unsigned index)
    __attribute__((warn_unused_result)) __attribute__((nonnull));


#define for_each_shm_obj(handle, obj, i)    \
    for ((i) = 0;   \
         (i) < (handle)->desc->attr.n_entry && ((obj) =shm_obj_ptr(handle, i)) != NULL;  \
         (i)++)



    /*
     * for non atomic shm, reader should allocate buffer which has size of shm_buf_size,
     * initialize the buf by shm_buf_init,
     * call shm_obj_copy to copy shm entry,
     * and reference shm object by SHM_PTR(buf).
     *
     * In order to avoid large copy, shm increases its age after every write.
     * Readers can cache the shm buf, pass the cached buf to shm_obj_copy.
     * If age not changed, the shm buf will not get copied.
     */

    /* 0 can not be shm age */
#define SHM_AGE_NONE    (0)

    /* shm will start age from SHM_AGE_START */
#define SHM_AGE_START   (SHM_AGE_NONE + 1)

    /* pseudo shm entry, prepended by age and index*/
    struct shm_pentry
    {
        int             age;
        unsigned        index;
        char                buf[0] __attribute__((aligned(SHM_ALIGN)));
    };

    /*
     * get buffer size of shm entry
     * @handle: shm handle
     *
     * Return value: -1 failed
     *  otherwise the size of buffer.
     */
    static inline int __attribute__((warn_unused_result)) __attribute__((nonnull))
    shm_buf_size(shm_handle_t handle)
    {
        assert(handle);

        return sizeof(struct shm_pentry) + handle->desc->attr.entry_size;
    }

    /*
     * get buffer size of shm entry
     * @size: original size of object
     *
     * Return value: size of buffer.
     */
    static inline size_t __attribute__((warn_unused_result)) __attribute__((nonnull))
    shm_buf_size2(size_t size)
    {
        return sizeof(struct shm_pentry) + SHM_SIZE(size);
    }


    /*
     * initialize the buf allocated, setting age to initial age so that copy is forced.
     * @buf: buffer which has size of shm_buf_size
     */
    static inline void __attribute__((nonnull)) shm_buf_init(void *buf)
    {
        struct shm_pentry *pentry = (struct shm_pentry *)buf;

        assert(buf);
        pentry->age = SHM_AGE_NONE;
    }

    /*
     * copy shm entry to buf,
     * @handle: shm handle
     * @index: index of shm entry
     * @buf: buffer to copy to
     * @size: size of buf
     *
     * Return value: 0 shm not changed
     *  1: shm changed
     *  -1: failed.
     */
    int shm_obj_copy(shm_handle_t handle, unsigned index, void *buf, size_t size)
    __attribute__((nonnull));

#define SHM_PTR(p)          (void *)(((struct shm_pentry *)(p))->buf)
#define SHM_CONTAINER(p)    ((void *)container_of(p, struct shm_pentry, buf))

#if defined(__cplusplus)
}
#endif


#endif

