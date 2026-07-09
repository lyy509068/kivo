#include "config.h"
#include <stdio.h>
#include <string.h>
#include <stdlib.h>

static int parse_persistence(const char *val) {
    if (strcmp(val, "AOF") == 0)  return 1;
    return 0;  
}

static int parse_onoff(const char *val) {
    return (strcmp(val, "ON") == 0) ? 1 : 0;
}

static int parse_replication(const char *val) {
    if (strcmp(val, "MASTER") == 0) return 1;
    if (strcmp(val, "SLAVE") == 0)  return 2;
    return 0;
}

int load_config(const char *filename, server_config_t *cfg) {
    cfg->port = 2000;
    cfg->persistence = 0;
    cfg->snapshot = 0;
    cfg->expire = 0;
    cfg->mempool = 0;
    cfg->replication = 0;

    FILE *fp = fopen(filename, "r");
    if (!fp) {
        printf("[Config] Cannot open %s, using defaults\n", filename);
        return -1;
    }

    char line[256];
    while (fgets(line, sizeof(line), fp)) {
        if (line[0] == '#' || line[0] == '\n' || line[0] == '\r') continue;

        char key[64], val[64];
        if (sscanf(line, "%63s %63s", key, val) == 2) {
            if (strcmp(key, "port") == 0)          cfg->port = atoi(val);
            if (strcmp(key, "persistence") == 0)   cfg->persistence = parse_persistence(val);
            if (strcmp(key, "snapshot") == 0)      cfg->snapshot = parse_onoff(val);
            if (strcmp(key, "expire") == 0)        cfg->expire = parse_onoff(val);
            if (strcmp(key, "mempool") == 0)       cfg->mempool = parse_onoff(val);
            if (strcmp(key, "replication") == 0)   cfg->replication = parse_replication(val);
        }
    }
    fclose(fp);

    printf("[Config] port=%d persistence=%d snapshot=%d expire=%d mempool=%d replication=%d\n",
           cfg->port, cfg->persistence, cfg->snapshot, cfg->expire, cfg->mempool, cfg->replication);
    return 0;
}