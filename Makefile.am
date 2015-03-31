AUTOMAKE_OPTIONS=foreign
sbin_PROGRAMS=memcachedb
memcachedb_SOURCES=memcachedb.c item.c memcachedb.h thread.c bdb.c stats.c assoc.c delay_log.c shm.c jenkins_hash.c
memcachedb_CFLAGS = -D_FILE_OFFSET_BITS=64 -D_LARGE_FILE
memcachedb_LDFLAGS = -lpthread -lrt -ldl -levent -ldb
memcachedb_LDADD = 
SUBDIRS=doc tools conf
