#ifndef __DELAY_H__
#define __DELAY_H__

#define DELAY_ADD 0
#define DELAY_DELETE 1

int delay_log_init(char *dir);
int insert_delay_log(char *key, size_t nkey, void *value, size_t valuesz, int type);
void delay_log_flush();

#endif