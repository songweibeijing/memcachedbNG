#ifndef __ASSOC_H__
#define __ASSOC_H__

/* associative array */
void assoc_init(const int hashpower_init);
item *assoc_find(const char *key, const size_t nkey, const uint32_t hv);
int assoc_insert(item *item, const uint32_t hv);
void assoc_delete(const char *key, const size_t nkey, const uint32_t hv);
void assoc_delete_keyvalue(const char *key, const size_t nkey, const uint32_t hv, const char *value, const size_t nvalue);
extern unsigned int hashpower;

#endif
