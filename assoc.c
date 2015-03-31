/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 * Hash table
 *
 * The hash function used here is by Bob Jenkins, 1996:
 *    <http://burtleburtle.net/bob/hash/doobs.html>
 *       "By Bob Jenkins, 1996.  bob_jenkins@burtleburtle.net.
 *       You may use this code any way you wish, private, educational,
 *       or commercial.  It's free."
 *
 * The rest of the file is licensed under the BSD license.  See LICENSE.
 */

#include "memcachedb.h"
#include "jenkins_hash.h"
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

#define hash jenkins_hash
#define HASHPOWER_DEFAULT 10240

typedef  unsigned long  int  ub4;   /* unsigned 4-byte quantities */
typedef  unsigned       char ub1;   /* unsigned 1-byte quantities */

/* how many powers of 2's worth of buckets we use */
unsigned int hashpower = HASHPOWER_DEFAULT;
static pthread_mutex_t object_mutex[HASHPOWER_DEFAULT];

#define hashsize(n) ((ub4)1<<(n))
#define hashmask(n) (hashsize(n)-1)

/* Main hash table. This is where we look except during expansion. */
static item **primary_hashtable = 0;

/* Number of items in the hash table. */
static unsigned int hash_items = 0;

/*
 * During expansion we migrate values with bucket granularity; this is how
 * far we've gotten so far. Ranges from 0 .. hashsize(hashpower - 1) - 1.
 */
static unsigned int expand_bucket = 0;

void assoc_init(const int hashtable_init)
{
    int i = 0;
    if (hashtable_init)
    {
        hashpower = hashtable_init;
    }
    primary_hashtable = calloc(hashsize(hashpower), sizeof(void *));
    if (! primary_hashtable)
    {
        fprintf(stderr, "Failed to init hashtable.\n");
        exit(EXIT_FAILURE);
    }

    for (i = 0; i < HASHPOWER_DEFAULT; i++)
    {
        pthread_mutex_init(&object_mutex[i], NULL);
    }
}

item *assoc_find(const char *key, const size_t nkey, const uint32_t hv)
{
    item *it = NULL, *ret = NULL;
    int depth = 0;
    uint32_t lock_hash = hv % HASHPOWER_DEFAULT;

    pthread_mutex_lock(&object_mutex[lock_hash]);
    it = primary_hashtable[hv & hashmask(hashpower)];
    while (it)
    {
        if ((nkey == it->nkey) && (memcmp(key, ITEM_key(it), nkey) == 0))
        {
            ret = it;
            break;
        }
        it = it->h_next;
        ++depth;
    }
    if (ret)
    {
        item_ref(ret);
    }
    pthread_mutex_unlock(&object_mutex[lock_hash]);

    return ret;
}

/* returns the address of the item pointer before the key.  if *item == 0,
   the item wasn't found */

static item **_hashitem_before_keyvalue(const char *key, const size_t nkey, const uint32_t hv, const char *value, const size_t nvalue)
{
    item **pos = NULL;
    pos = &primary_hashtable[hv & hashmask(hashpower)];
    while (*pos && ((nkey != (*pos)->nkey) || memcmp(key, ITEM_key(*pos), nkey) || memcmp(value, ITEM_data(*pos), nvalue)))
    {
        pos = &(*pos)->h_next;
    }
    return pos;
}

/* returns the address of the item pointer before the key.  if *item == 0,
   the item wasn't found */

static item **_hashitem_before(const char *key, const size_t nkey, const uint32_t hv)
{
    item **pos = NULL;
    pos = &primary_hashtable[hv & hashmask(hashpower)];
    while (*pos && ((nkey != (*pos)->nkey) || memcmp(key, ITEM_key(*pos), nkey)))
    {
        pos = &(*pos)->h_next;
    }
    return pos;
}

/* Note: this isn't an assoc_update.  The key must not already exist to call this */
int assoc_insert(item *it, const uint32_t hv)
{
    uint32_t lock_hash = hv % HASHPOWER_DEFAULT;

    pthread_mutex_lock(&object_mutex[lock_hash]);
    it->refer_count++;
    it->h_next = primary_hashtable[hv & hashmask(hashpower)];
    primary_hashtable[hv & hashmask(hashpower)] = it;
    hash_items++;
    item_ref(it);
    pthread_mutex_unlock(&object_mutex[lock_hash]);

    return 1;
}

void assoc_delete(const char *key, const size_t nkey, const uint32_t hv)
{
    uint32_t lock_hash = hv % HASHPOWER_DEFAULT;
    pthread_mutex_lock(&object_mutex[lock_hash]);

    item **before = _hashitem_before(key, nkey, hv);
    if (*before)
    {
        item *nxt, *del;
        hash_items--;
        del = (*before);
        nxt = del->h_next;
        del->h_next = 0;
        *before = nxt;
        item_free(del);
    }
    pthread_mutex_unlock(&object_mutex[lock_hash]);
    return;
}

void assoc_delete_keyvalue(const char *key, const size_t nkey, const uint32_t hv, const char *value, const size_t nvalue)
{
    uint32_t lock_hash = hv % HASHPOWER_DEFAULT;
    pthread_mutex_lock(&object_mutex[lock_hash]);

    item **before = _hashitem_before_keyvalue(key, nkey, hv, value, nvalue);
    if (*before)
    {
        item *nxt;
        hash_items--;

        nxt = (*before)->h_next;
        (*before)->h_next = 0;   /* probably pointless, but whatever. */
        *before = nxt;
    }
    pthread_mutex_unlock(&object_mutex[lock_hash]);
    return;
}

