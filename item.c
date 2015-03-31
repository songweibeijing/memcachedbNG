/*
 *  MemcacheDB - A distributed key-value storage system designed for persistent:
 *
 *      http://memcachedb.googlecode.com
 *
 *  The source code of Memcachedb is most based on Memcached:
 *
 *      http://danga.com/memcached/
 *
 *  Copyright 2008 Steve Chu.  All rights reserved.
 *
 *  Use and distribution licensed under the BSD license.  See
 *  the LICENSE file for full text.
 *
 *  Authors:
 *      Steve Chu <stvchu@gmail.com>
 *
 */

#include "memcachedb.h"
#include "jenkins_hash.h"
#include <stdio.h>
#include <errno.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <pthread.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <stdlib.h>

#define MAX_ITEM_FREELIST_LENGTH 4000
#define INIT_ITEM_FREELIST_LENGTH 500

static size_t item_make_header(const uint8_t nkey, const int flags, const int nbytes, char *suffix, uint8_t *nsuffix);

static item **freeitem;
static int freeitemtotal;
static int freeitemcurr;

void item_ref(item *it)
{
    if (it)
    {
        it->refer_count++;
    }
}

void item_unref(item *it)
{
    if (it)
    {
        it->refer_count--;
    }
}

void item_init(void)
{
    freeitemtotal = INIT_ITEM_FREELIST_LENGTH;
    freeitemcurr  = 0;

    freeitem = (item **)malloc(sizeof(item *) * freeitemtotal);
    if (freeitem == NULL)
    {
        perror("malloc()");
    }
    return;
}

/*
 * Returns a item buffer from the freelist, if any. Sholud call
 * item_from_freelist for thread safty.
 * */
item *do_item_from_freelist(void)
{
    item *s;

    if (freeitemcurr > 0)
    {
        s = freeitem[--freeitemcurr];
    }
    else
    {
        /* If malloc fails, let the logic fall through without spamming
         * STDERR on the server. */
        s = (item *)malloc(settings.item_buf_size);
        if (s != NULL)
        {
            memset(s, 0, settings.item_buf_size);
        }
    }

    return s;
}

/*
 * Adds a item to the freelist. Should call
 * item_add_to_freelist for thread safty.
 */
int do_item_add_to_freelist(item *it)
{
    if (freeitemcurr < freeitemtotal)
    {
        freeitem[freeitemcurr++] = it;
        return 0;
    }
    else
    {
        if (freeitemtotal >= MAX_ITEM_FREELIST_LENGTH)
        {
            return 1;
        }
        /* try to enlarge free item buffer array */
        item **new_freeitem = (item **)realloc(freeitem, sizeof(item *) * freeitemtotal * 2);
        if (new_freeitem)
        {
            freeitemtotal *= 2;
            freeitem = new_freeitem;
            freeitem[freeitemcurr++] = it;
            return 0;
        }
    }
    return 1;
}

/**
 * Generates the variable-sized part of the header for an object.
 *
 * key     - The key
 * nkey    - The length of the key
 * flags   - key flags
 * nbytes  - Number of bytes to hold value and addition CRLF terminator
 * suffix  - Buffer for the "VALUE" line suffix (flags, size).
 * nsuffix - The length of the suffix is stored here.
 *
 * Returns the total size of the header.
 */
static size_t item_make_header(const uint8_t nkey, const int flags, const int nbytes,
                               char *suffix, uint8_t *nsuffix)
{
    /* suffix is defined at 40 chars elsewhere.. */
    *nsuffix = (uint8_t) snprintf(suffix, 40, " %d %d\r\n", flags, nbytes - 2);
    return sizeof(item) + nkey + *nsuffix + nbytes;
}

/*
 * alloc a item buffer, and init it.
 */
item *item_alloc1(char *key, const size_t nkey, const int flags, const int nbytes)
{
    uint8_t nsuffix;
    item *it;
    char suffix[40];
    size_t ntotal = item_make_header(nkey + 1, flags, nbytes, suffix, &nsuffix);

    if (ntotal > settings.item_buf_size)
    {
        it = (item *)malloc(ntotal);
        if (it == NULL)
        {
            return NULL;
        }
        memset(it, 0, ntotal);
        if (settings.verbose > 1)
        {
            fprintf(stderr, "alloc a item buffer from malloc.\n");
        }
    }
    else
    {
        it = item_from_freelist();
        if (it == NULL)
        {
            return NULL;
        }
        if (settings.verbose > 1)
        {
            fprintf(stderr, "alloc a item buffer from freelist.\n");
        }
    }

    it->nkey = nkey;
    it->nbytes = nbytes;
    strcpy(ITEM_key(it), key);
    memcpy(ITEM_suffix(it), suffix, (size_t)nsuffix);
    it->nsuffix = nsuffix;

    item_ref(it);
    return it;
}

/*
 * alloc a item buffer only.
 */
item *item_alloc2(size_t ntotal)
{
    item *it;
    if (ntotal > settings.item_buf_size)
    {
        it = (item *)malloc(ntotal);
        if (it == NULL)
        {
            return NULL;
        }
        memset(it, 0, ntotal);
        if (settings.verbose > 1)
        {
            fprintf(stderr, "alloc a item buffer from malloc.\n");
        }
    }
    else
    {
        it = item_from_freelist();
        if (it == NULL)
        {
            return NULL;
        }
        if (settings.verbose > 1)
        {
            fprintf(stderr, "alloc a item buffer from freelist.\n");
        }
    }

    item_ref(it);
    return it;
}

/*
 * free a item buffer. here 'it' must be a full item.
 */

int item_free(item *it)
{
    size_t ntotal = 0;
    if (NULL == it)
    {
        return 0;
    }

    item_unref(it);
    if (it->refer_count > 0)
    {
        return 0;
    }

    /* ntotal may be wrong, if 'it' is not a full item. */
    ntotal = ITEM_ntotal(it);
    if (ntotal > settings.item_buf_size)
    {
        if (settings.verbose > 1)
        {
            fprintf(stderr, "ntotal: %d, use free() directly.\n", ntotal);
        }
        free(it);
    }
    else
    {
        if (0 != item_add_to_freelist(it))
        {
            if (settings.verbose > 1)
            {
                fprintf(stderr, "ntotal: %d, add a item buffer to freelist fail, use free() directly.\n", ntotal);
            }
            free(it);
        }
        else
        {
            if (settings.verbose > 1)
            {
                fprintf(stderr, "ntotal: %d, add a item buffer to freelist.\n", ntotal);
            }
        }
    }
    return 0;
}

static int is_sync(item *it)
{
    if ((it->item_status & ITEM_SYNC) == 0)
    {
        return 0;
    }
    else
    {
        return 1;
    }
}

/* if return item is not NULL, free by caller */
item *item_get(char *key, size_t nkey)
{
    item *it = NULL;

    uint32_t kv = jenkins_hash(key, nkey);
    it = assoc_find(key, nkey, kv);

    if (it != NULL)
    {
        if ((it->item_status & ITEM_DELETE) != 0)
        {
            if (is_sync(it))
            {
                assoc_delete(key, nkey, kv);
                item_free(it);
            }
            return NULL;
        }
        else
        {
            return it;
        }
    }
    else
    {
        /* first, alloc a fixed size */
        it = item_alloc2(settings.item_buf_size);
        if (it == 0)
        {
            return NULL;
        }

        it = bdb_get(key, nkey, it, settings.item_buf_size);
        return it;
    }
}

item *item_cget(DBC *cursorp, char *start, size_t nstart, u_int32_t flags)
{
    item *it = NULL;
    it = bdb_cget(cursorp, start, nstart, flags);
    return it;
}

/* 0 for Success
   -1 for SERVER_ERROR
   */
int item_put(char *key, size_t nkey, item *it)
{
    int ret;
    item *obj = NULL;

    it->item_status = ITEM_PUT;
    uint32_t kv = jenkins_hash(key, nkey);
    obj = assoc_find(key, nkey, kv);

    if (obj == NULL)
    {
        assoc_insert(it, kv);
    }
    else
    {
        assoc_delete(key, nkey, kv);
        item_free(obj);
        assoc_insert(it, kv);
    }

    ret = insert_delay_log(key, nkey, it, ITEM_ntotal(it), DELAY_ADD);
    if (ret != 0)
    {
        fprintf(stderr, "dbp->put: %d\n", ret);
        if (settings.verbose > 1)
        {
            fprintf(stderr, "dbp->put: %d\n", ret);
        }
        return -1;
    }

    return 0;
}

/* 0 for Success
   1 for NOT_FOUND
   -1 for SERVER_ERROR
   */
int item_delete(char *key, size_t nkey)
{
    int ret, exist = 0;
    item *obj = NULL;

    uint32_t kv = jenkins_hash(key, nkey);
    obj = assoc_find(key, nkey, kv);

    if (obj == NULL)
    {
        exist = bdb_exists(key, nkey);
    }
    else
    {
        if ((obj->item_status & ITEM_DELETE) != 0)
        {
            exist = 0;
        }
        else
        {
            printf("deleted 1111, marke delete *****\n");
            obj->item_status = ITEM_DELETE;
            exist = 1;
        }
        item_free(obj);
    }

    if (exist)
    {
        ret = insert_delay_log(key, nkey, NULL, 0, DELAY_DELETE);
        if (ret != 0)
        {
            if (settings.verbose > 1)
            {
                fprintf(stderr, "dbp->put: %d\n", ret);
            }
            return -1;
        }
        return 0;
    }
    else
    {
        return 1;
    }
}

/*
   1 for exists
   0 for non-exist
   */
int item_exists(char *key, size_t nkey)
{
    int ret = 0;

    uint32_t kv = jenkins_hash(key, nkey);
    item *it = assoc_find(key, nkey, kv);

    if (it != NULL)
    {
        if ((it->item_status & ITEM_DELETE) != 0)
        {
            ret = 0;
        }
        else
        {
            ret = 1;
        }

        item_free(it);

        printf("key : %s, exist1 %d\n", key, ret);
        return ret;
    }

    ret = bdb_exists(key, nkey);
    printf("key : %s, exist2 %d\n", key, ret);
    return ret;
}

