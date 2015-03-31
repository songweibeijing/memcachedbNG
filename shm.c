#include "shm_api.h"

#define ARRAY_SIZE(x) (sizeof(x) / sizeof((x)[0]))

/*
 * fds opened in constructor has small values( 3 4 5)
 * and may be dupped in some programs(postfix).
 * dup shm fds to higher position to avoid this situation.
 */
#define SHM_FD_BASE         (37)

static void shm_handle_dup(struct shm_handle *handle)
{
    int newfd;

    assert(handle->fd >= 0);
    if (handle->fd > SHM_FD_BASE)
    {
        return;
    }

    newfd = fcntl(handle->fd, F_DUPFD, SHM_FD_BASE);
    if (newfd > 0)
    {
        printf("function %s successful, old fd %d, new fd %d\n",
               __FUNCTION__, handle->fd, newfd);
        close(handle->fd);
        handle->fd = newfd;
    }
}

static inline int shm_attr_same(struct shm_attr *attr1, struct shm_attr *attr2)
{
    return attr1->meta_size == attr2->meta_size &&
           attr1->n_entry == attr2->n_entry &&
           attr1->entry_size == attr2->entry_size;
}

static inline struct shm_handle *shm_handle_alloc(char *name)
{
    struct shm_handle *handle;

    handle = calloc(1, sizeof(*handle));
    if (!handle)
    {
        return NULL;
    }

    handle->fd = -1;
    pthread_rwlock_init(&handle->rwlock, NULL);
    strncpy(handle->name, name, sizeof(handle->name) - 1);
    return handle;
}

static inline void shm_handle_free(struct shm_handle *handle)
{
    pthread_rwlock_destroy(&handle->rwlock);
    free(handle);
}

static inline size_t shm_size(struct shm_attr *attr)
{
    size_t size;

    size = sizeof(struct shm_descriptor) + attr->meta_size +
           attr->n_entry * attr->entry_size;

    /* overflow check */
    assert(size > sizeof(struct shm_descriptor));
    return size;
}

static inline void shm_unlock(shm_handle_t handle)
{
    assert(handle);

    flock(handle->fd, LOCK_UN);
    pthread_rwlock_unlock(&handle->rwlock);
}

void shm_rlock(shm_handle_t handle)
{
    assert(handle);

    pthread_rwlock_rdlock(&handle->rwlock);
    flock(handle->fd, LOCK_SH);
}

void shm_runlock(shm_handle_t handle)
{
    assert(handle);
    shm_unlock(handle);
}

static void shm_wlock_internal(shm_handle_t handle)
{
    assert(handle);

    pthread_rwlock_wrlock(&handle->rwlock);
    flock(handle->fd, LOCK_EX);
}

static inline struct shm_wlocker *__shm_get_wlocker(shm_handle_t handle)
{
    assert(handle);

    return &handle->desc->wlocker;
}

void __shm_wlock(shm_handle_t handle, pid_t pid, const char *func, int line)
{
    struct shm_wlocker *wlocker;

    shm_wlock_internal(handle);

    wlocker = __shm_get_wlocker(handle);
    /* log the lock holder */
    wlocker->pid = pid;
    strncpy(wlocker->func, func, sizeof(wlocker->func) - 1);
    wlocker->line = line;
}

static void shm_wunlock_internal(shm_handle_t handle)
{
    assert(handle);

    /* increase age */
    if (handle->desc)
    {
        handle->desc->age++;
        if (handle->desc->age == SHM_AGE_NONE)
        {
            handle->desc->age++;
        }
    }

    shm_unlock(handle);
}

void __shm_wunlock(shm_handle_t handle)
{
    struct shm_wlocker *wlocker;

    wlocker = __shm_get_wlocker(handle);
    wlocker->pid = 0;
    wlocker->func[0] = 0;
    wlocker->line = 0;

    shm_wunlock_internal(handle);
}

int shm_get_wlocker(const char *name, struct shm_wlocker *wlocker)
{
    int fd;
    struct shm_descriptor desc;
    int ret;
    size_t size;

    assert(name);

    ret = -1;
    fd = shm_open(name, O_RDWR, 0777);
    if (fd < 0)
    {
        goto out;
    }
    size = read(fd, &desc, sizeof(desc));
    if (size != sizeof(desc))
    {
        goto close_out;
    }
    memcpy(wlocker, &desc.wlocker, sizeof(*wlocker));
    ret = 0;

close_out:
    close(fd);
out:
    return ret;
}

shm_handle_t shm_create(char *name, struct shm_attr *attr)
{
    struct shm_handle *handle;
    size_t size;

    assert(name && attr);
    assert(attr->n_entry && attr->entry_size);

    handle = shm_handle_alloc(name);
    if (!handle)
    {
        goto err;
    }

    attr->meta_size = SHM_SIZE(attr->meta_size);
    attr->entry_size = SHM_SIZE(attr->entry_size);

    handle->fd = shm_open(handle->name, O_RDWR | O_CREAT | O_EXCL, 0777);
    if (handle->fd < 0)
    {
        goto free_err;
    }
    shm_handle_dup(handle);

    shm_wlock_internal(handle);
    size = shm_size(attr);
    if (ftruncate(handle->fd, size) < 0)
    {
        goto close_err;
    }

    handle->desc = mmap(NULL, size, PROT_READ | PROT_WRITE, MAP_SHARED, handle->fd, 0);
    if (handle->desc == MAP_FAILED)
    {
        handle->desc = NULL;
        goto close_err;
    }

    handle->desc->age = SHM_AGE_START;
    handle->desc->attr = *attr;

    shm_wunlock_internal(handle);
    return handle;

close_err:
    shm_wunlock_internal(handle);
    shm_unlink(handle->name);
    close(handle->fd);

free_err:
    shm_handle_free(handle);

    /*
     * may already be created, config daemon restarted.
     * try to attach it.
     */

    handle = shm_attach(name);
    if (handle == SHM_HANDLE_FAIL)
    {
        return handle;
    }
    /* check if attr is same */
    if (shm_attr_same(&handle->desc->attr, attr))
    {
        return handle;
    }

    shm_detach(handle);

err:
    return SHM_HANDLE_FAIL;
}

void shm_destroy(char *name)
{
    assert(name);
    shm_unlink(name);
}

shm_handle_t shm_attach(char *name)
{
    struct shm_handle *handle;
    struct shm_descriptor desc;
    int ret;
    size_t size;

    assert(name);
    handle = shm_handle_alloc(name);
    if (!handle)
    {
        goto err;
    }

    handle->fd = shm_open(handle->name, O_RDWR, 0777);
    if (handle->fd < 0)
    {
        goto free_err;
    }
    shm_handle_dup(handle);

    shm_rlock(handle);
    ret = read(handle->fd, &desc, sizeof(desc));
    shm_runlock(handle);

    if (ret != sizeof(desc))
    {
        goto close_err;
    }

    size = shm_size(&desc.attr);

    handle->desc = mmap(NULL, size, PROT_READ | PROT_WRITE, MAP_SHARED, handle->fd, 0);
    if (handle->desc == MAP_FAILED)
    {
        handle->desc = NULL;
        goto close_err;
    }

    return handle;

close_err:
    close(handle->fd);

free_err:
    shm_handle_free(handle);

err:
    return SHM_HANDLE_FAIL;

}

void shm_detach(shm_handle_t handle)
{
    assert(handle && handle->desc);

    munmap(handle->desc, shm_size(&handle->desc->attr));
    close(handle->fd);
    shm_handle_free(handle);
}

void *shm_meta_ptr(shm_handle_t handle)
{
    struct shm_attr *attr;

    assert(handle);
    attr = &handle->desc->attr;
    if (!attr->meta_size)
    {
        return NULL;
    }

    return handle->desc->buf;
}

void *shm_obj_ptr(shm_handle_t handle,  unsigned index)
{
    struct shm_attr *attr;
    char *base;

    assert(handle);
    attr = &handle->desc->attr;
    assert(attr->n_entry > index);

    base = handle->desc->buf + attr->meta_size;
    base += attr->entry_size * index;
    return (void *)base;
}

int shm_obj_copy(shm_handle_t handle, unsigned index, void *buf, size_t size)
{
    struct shm_attr *attr;
    struct shm_descriptor *desc;
    struct shm_pentry *pentry;
    void *obj;

    assert(handle && buf);

    desc =  handle->desc;
    attr = &handle->desc->attr;
    assert(attr->n_entry > index);
    assert(size >= shm_buf_size(handle));

    pentry = buf;
    /* Not changed */
    if (desc->age == pentry->age && index == pentry->index)
    {
        return 0;
    }

    /* slow path: the shm instance has been updated */
    shm_rlock(handle);
    obj = shm_obj_ptr(handle, index);
    memcpy(pentry->buf, obj, attr->entry_size);
    pentry->age = desc->age;
    pentry->index = index;
    shm_runlock(handle);

    return 1;
}


