#include "memcachedb.h"
#include <sys/stat.h>
#include <sys/socket.h>
#include <sys/signal.h>
#include <sys/resource.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <errno.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <assert.h>
#include <pthread.h>

#include "jenkins_hash.h"
#include "shm_api.h"
#include "linux_list.h"

#define MAX_PATH 1024
#define THREAD_STACK_SIZE (1024*1024*4)
#define MAX_RECORD_NUM 1024
#define MAX_FILE_NUM 1000000
#define DEFER_LOG_PURGE_INTERVAL 60
#define SHM_CURRENT_FILE_STATUS "/shm_currfile_status"

static pthread_mutex_t log_mutex;
static pthread_mutex_t delay_mutex;
static pthread_cond_t delay_condvar;
struct list_head delay_jobs;
static unsigned long long delay_pending = 0;
int g_delay = 0;
static int g_curr_rcno = 0;
static char g_curr_file[MAX_PATH];
static char g_dir[MAX_PATH];
static int g_first_time = 1;

typedef struct delay_item
{
    char *key;
    size_t nkey;
    void *value;
    size_t nvalue;
    int type;
} delay_item;

#define DELAY_INSERT 1
#define SYSTEM_TYPE  2

typedef struct delay_job
{
    delay_item *item;
    int type;
    int recno;
    char filepath[MAX_PATH];
    struct list_head list;
} delay_job;

typedef struct delay_process_status
{
    //info about last file open
    int last_file_number;
    char last_file_name[MAX_PATH];

    //info about curr file which is being processed.
    int curr_file_number;
    char curr_file_name[MAX_PATH];
    int curr_recno; //the record number will be processed.
} delay_process_status;
delay_process_status *g_delay_process_status = NULL;

static void *delay_process_jobs(void *arg);
static int init_currfile_shm(void);
static int process_leftover_delay_logs(char *dir);
static int put_delay_item(delay_item *item);
static int deserialize_item(char *buf, delay_item *item);

/* Initialize the background system, spawning the thread. */
int delay_log_init(char *dir)
{
    pthread_attr_t attr;
    pthread_t thread;
    size_t stacksize;

    snprintf(g_dir, MAX_PATH, "%s", dir);
    if (init_currfile_shm() != 0)
    {
        return -1;
    }

    pthread_mutex_init(&delay_mutex, NULL);
    pthread_cond_init(&delay_condvar, NULL);
    INIT_LIST_HEAD(&delay_jobs);
    delay_pending = 0;

    printf("begin to process leftover defer logs\n");
    if (process_leftover_delay_logs(dir) != 0)
    {
        return -1;
    }

    /* Set the stack size as by default it may be small in some system */
    pthread_attr_init(&attr);
    pthread_attr_getstacksize(&attr, &stacksize);
    if (!stacksize)
    {
        stacksize = 1;    /* The world is full of Solaris Fixes */
    }
    while (stacksize < THREAD_STACK_SIZE)
    {
        stacksize *= 2;
    }
    pthread_attr_setstacksize(&attr, stacksize);
    if (pthread_create(&thread, &attr, delay_process_jobs, NULL) != 0)
    {
        printf("Fatal: Can't initialize Background Jobs.");
        return -1;
    }
    return 0;
}

static int init_currfile_shm(void)
{
    /* init system counter SHM */
    shm_handle_t handle;
    struct shm_attr attr;
    int trunc = 0;

    memset(&attr, 0, sizeof(attr));
    attr.n_entry = 1;
    attr.entry_size = sizeof(delay_process_status);
    handle = shm_attach(SHM_CURRENT_FILE_STATUS);
    if (handle == SHM_HANDLE_FAIL)
    {
        /* for SHM size changed issue */
        handle = shm_create(SHM_CURRENT_FILE_STATUS, &attr);
        if (handle == SHM_HANDLE_FAIL)
        {
            printf("init_currfile_shm: cannot create shm %s\n", SHM_CURRENT_FILE_STATUS);
            return -1;
        }
        trunc = 1;
    }
    g_delay_process_status = (delay_process_status *)shm_obj_ptr(handle, 0);
    if (trunc == 1)
    {
        memset(g_delay_process_status, 0, sizeof(delay_process_status));
    }
    return 0;
}

static size_t file_size(char *path)
{
    struct stat buf;
    int ret = stat(path, &buf);
    if (ret < 0)
    {
        return 0;
    }
    return buf.st_size;
}

static char *read_file(char *path)
{
    size_t filesz = file_size(path);
    if (filesz == 0)
    {
        return NULL;
    }

    char *filebuf = (char *)calloc(1, filesz + 1);
    if (filebuf == NULL)
    {
        return NULL;
    }

    int fd = open(path, O_RDONLY);
    if (fd <= 0)
    {
        free(filebuf);
        return NULL;
    }
    read(fd, filebuf, filesz);
    close(fd);
    return filebuf;
}

static int get_next_number(int number)
{
    return (number + 1) % MAX_FILE_NUM;
}

static void get_file_name(char *dir, int number, char *buf)
{
    snprintf(buf, MAX_PATH, "%s/%08d", dir, number);
}

static int update_next_file(char *file, int recno)
{
    char tmp[MAX_PATH];
    int file_num = 0;
    sscanf(file, "%s/%08d", (char *)&tmp, &file_num);
    snprintf(g_delay_process_status->curr_file_name, MAX_PATH, "%s", file);
    g_delay_process_status->curr_recno = recno;
    g_delay_process_status->curr_file_number = file_num;
    return 0;
}

static void delete_delay_item(delay_item *item);
static delay_item *new_delay_item(char *key, size_t nkey, void *value, size_t valuesz, int type)
{
    delay_item *item = (delay_item *) calloc(1, sizeof(delay_item));
    if (item == NULL)
    {
        return NULL;
    }

    item->type = type;
    item->key = strdup(key);
    if (!item->key)
    {
        goto failed;
    }

    item->nkey = nkey;
    item->value = calloc(1, valuesz);
    if (!item->value)
    {
        goto failed;
    }

    memcpy(item->value, value, valuesz);
    item->nvalue = valuesz;
    return item;

failed:
    delete_delay_item(item);
    return NULL;
}

static void delete_delay_item(delay_item *item)
{
    if (item)
    {
        if (item->key)
        {
            free(item->key);
        }
        if (item->value)
        {
            free(item->value);
        }
        free(item);
    }
}

static int item_length(delay_item *item)
{
    return item->nkey + item->nvalue + sizeof(item->type) + sizeof(size_t) * 4;
}

static char *serialize_item(delay_item *item)
{
    char *buf = NULL, *tmp = NULL;
    size_t bufsize = item_length(item);

    buf = (char *)calloc(1, bufsize);
    if (buf == NULL)
    {
        return NULL;
    }
    tmp = buf;

    memcpy(tmp, &bufsize, sizeof(size_t));
    tmp += sizeof(size_t);
    memcpy(tmp, &item->nkey, sizeof(size_t));
    tmp += sizeof(size_t);
    memcpy(tmp, item->key, item->nkey);
    tmp += item->nkey;
    memcpy(tmp, &item->nvalue, sizeof(size_t));
    tmp += sizeof(size_t);
    memcpy(tmp, item->value, item->nvalue);
    tmp += item->nvalue;
    memcpy(tmp, &item->type, sizeof(size_t));
    tmp += sizeof(size_t);
    return buf;
}

static int deserialize_item(char *buf, delay_item *item)
{
    size_t bufsz = 0;

    memcpy(&bufsz, buf, sizeof(size_t));
    buf += sizeof(size_t);
    memcpy(&item->nkey, buf, sizeof(size_t));
    buf += sizeof(size_t);
    item->key = (char *)calloc(1, item->nkey);
    if (!item->key)
    {
        goto failed;
    }

    memcpy(item->key, buf, item->nkey);
    buf += item->nkey;
    memcpy(&item->nvalue, buf, sizeof(size_t));
    buf += sizeof(size_t);
    item->value = calloc(1, item->nvalue);
    if (!item->value)
    {
        goto failed;
    }

    memcpy(item->value, buf, item->nvalue);
    buf += item->nvalue;
    memcpy(&item->type, buf, sizeof(size_t));
    return bufsz;

failed:
    if (item->key)
    {
        free(item->key);
    }
    if (item->value)
    {
        free(item->value);
    }
    return -1;
}


static int new_delay_file(char *dir, int number)
{
    char file[MAX_PATH] = {0};

    if (dir[strlen(dir) - 1] == '/')
    {
        dir[strlen(dir) - 1] = '\0';
    }
    get_file_name(dir, number, file);
    int fd = open(file, O_APPEND | O_CREAT | O_RDWR, 0644);
    if (fd < 0)
    {
        printf("open file %s failed, error : %s\n", file, strerror(errno));
        return -1;
    }

    if (g_delay > 0)
    {
        close(g_delay);
    }
    g_delay = fd;
    g_curr_rcno = 0;
    snprintf(g_curr_file, MAX_PATH, "%s", (char *)file);
    if (g_delay_process_status)
    {
        g_delay_process_status->last_file_number = number;
        snprintf(g_delay_process_status->last_file_name, MAX_PATH, "%s", file);
    }
    return 0;
}

static int process_delay_item(delay_item *ditem)
{
    uint32_t kv = jenkins_hash(ditem->key, ditem->nkey);
    item *obj = assoc_find(ditem->key, ditem->nkey, kv);
    int ret = 0;

    if (ditem->type == DELAY_ADD)
    {
        ret =  bdb_put(ditem->key, ditem->nkey, ditem->value, ditem->nvalue);
    }
    else if (ditem->type == DELAY_DELETE)
    {
        ret =  bdb_delete(ditem->key, ditem->nkey);
    }

    if (obj != NULL)
    {
        obj->item_status |= ITEM_SYNC;
    }

    return ret;
}

static int skip_delay_items(char *path, int num)
{
    size_t ret = 0;
    int fd;
    size_t filesz = file_size(path);;
    if (filesz == 0)
    {
        return 0;
    }

    fd = open(path, O_RDONLY);
    if (fd <= 0)
    {
        return 0;
    }

    ret = 0;
    while (num > 0 && ret < filesz)
    {
        size_t len = 0;
        if (read(fd, &len, sizeof(len)) == -1)
        {
            return -1;
        }
        if (lseek(fd, len - sizeof(len), SEEK_CUR) == -1)
        {
            return -1;
        }
        num--;
        ret += len;
    }
    close(fd);
    return ret;
}

static struct delay_job *new_delay_job(char *file, int fileno, int type, delay_item *item)
{
    if (item == NULL)
    {
        return NULL;
    }

    struct delay_job *job = (struct delay_job *)calloc(1, sizeof(struct delay_job));
    if (!job)
    {
        return NULL;
    }

    if (item)
    {
        job->item = item;
    }
    job->recno = fileno;
    job->type = type;
    if (file)
    {
        snprintf(job->filepath, MAX_PATH, "%s", file);
    }

    return job;
}

static void delete_delay_job(struct delay_job *job)
{
    if (job)
    {
        delay_item *item = job->item;
        delete_delay_item(item);
        free(job);
    }
}

static void add_to_process_queue(struct delay_job *job)
{
    INIT_LIST_HEAD(&job->list);
    pthread_mutex_lock(&delay_mutex);
    list_add_tail(&job->list, &delay_jobs);
    pthread_cond_signal(&delay_condvar);
    pthread_mutex_unlock(&delay_mutex);
    delay_pending++;
}

static int process_leftover_delay_log(char *path, int start_recno)
{
    delay_item *item = NULL;
    struct delay_job *job = NULL;

    char *filebuf = read_file(path);
    if (filebuf == NULL)
    {
        return 0;
    }
    size_t filesz = file_size(path);
    size_t startindex = skip_delay_items(path, start_recno);
    while (startindex < filesz)
    {
        int length = 0;
        item = (delay_item *)calloc(1, sizeof(delay_item));
        if (item == NULL)
        {
            goto failed;
        }

        length = deserialize_item(filebuf + startindex, item);
        if (length < 0)
        {
            free(item);
            goto failed;
        }

        job = new_delay_job(path, start_recno, DELAY_INSERT, item);
        if (job == NULL)
        {
            printf("failed to process file %s, record number %d\n", path, start_recno);
            delete_delay_item(item);
            goto failed;
        }

        startindex += length;
        start_recno++;
        add_to_process_queue(job);
    }

    job = new_delay_job(path, 0, SYSTEM_TYPE, NULL);
    if (job != NULL)
    {
        printf("failed to process file %s, record number %d\n", path, start_recno);
        add_to_process_queue(job);
    }

failed:
    if (filebuf)
    {
        free(filebuf);
    }
    return 0;
}

static int process_leftover_delay_logs(char *dir)
{
    char file[MAX_PATH] = {0};
    int ret, filenum =  0;
    int is_specail = 1;
    int is_last_file = 0;
    filenum = g_delay_process_status->curr_file_number;

    while (1)
    {
        if (filenum == g_delay_process_status->last_file_number)
        {
            is_last_file = 1;
        }

        get_file_name(dir, filenum, file);
        if (is_specail == 1)
        {
            ret = process_leftover_delay_log(file, g_delay_process_status->curr_recno);
            is_specail = 0;
        }
        else
        {
            ret = process_leftover_delay_log(file, 0);
        }
        if (ret == -1)
        {
            printf("process left over file %s failed\n", file);
        }

        if (is_last_file == 1)
        {
            printf("process all left over files\n");
            return 0;
        }

        filenum = get_next_number(filenum);
    }

    return 0;
}

int insert_delay_log(char *key, size_t nkey, void *value, size_t valuesz, int type)
{
    int ret = 0;
    char *buf = NULL;
    struct delay_job *job = NULL;

    delay_item *item = new_delay_item(key, nkey, value, valuesz, type);
    if (item == NULL)
    {
        return -1;
    }

    buf = serialize_item(item);
    if (buf == NULL)
    {
        delete_delay_item(item);
        return -1;
    }

    pthread_mutex_lock(&log_mutex);

    if (g_curr_rcno >= MAX_RECORD_NUM || g_first_time == 1)
    {
        int number = get_next_number(g_delay_process_status->last_file_number);
        new_delay_file(g_dir, number);
        g_first_time = 0;
    }

    ret = write(g_delay, buf, item_length(item));
    if (ret == item_length(item))
    {
        job = new_delay_job(g_curr_file, g_curr_rcno, DELAY_INSERT, item);
        if (job != NULL)
        {
            g_curr_rcno++;
        }
    }

    pthread_mutex_unlock(&log_mutex);

    if (ret != item_length(item) || job == NULL)
    {
        printf("failed to process file %s, record number %d\n", g_curr_file, g_curr_rcno);
        delete_delay_item(item);
        free(buf);
        return -1;
    }

    add_to_process_queue(job);
    free(buf);

    return 0;
}

void *delay_process_jobs(void *arg)
{
    struct delay_job *ln = NULL;

    pthread_detach(pthread_self());
    pthread_mutex_lock(&delay_mutex);
    while (1)
    {
        /* The loop always starts with the lock hold. */
        if (list_empty(&delay_jobs))
        {
            pthread_cond_wait(&delay_condvar, &delay_mutex);
            continue;
        }

        /* Pop the job from the queue. */
        ln = list_entry(delay_jobs.next, struct delay_job, list);
        pthread_mutex_unlock(&delay_mutex);

        if (ln->type == SYSTEM_TYPE)
        {
            //remove unused files.
            printf("begin to delete file :%s\n", ln->filepath);
            unlink(ln->filepath);
        }
        else if (ln->type == DELAY_INSERT)
        {
            if (process_delay_item(ln->item) == 0)
            {
                update_next_file(ln->filepath, ln->recno);
            }
            else
            {
                if (strlen(ln->filepath) > 0)
                {
                    printf("failed to process delay item:%s|%d\n", ln->filepath, ln->recno);
                }
                else
                {
                    printf("faield to process emtpy delay file\n");
                }
            }
        }

        pthread_mutex_lock(&delay_mutex);
        list_del(&ln->list);
        delete_delay_job(ln);

        delay_pending--;
    }
    return NULL;
}

/* Return the number of pending jobs of the specified type. */
unsigned long long delay_jobnum(int type)
{
    unsigned long long val;
    pthread_mutex_lock(&delay_mutex);
    val = delay_pending;
    pthread_mutex_unlock(&delay_mutex);
    return val;
}

