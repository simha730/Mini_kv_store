#define _GNU_SOURCE
#include <sys/socket.h>
#include <sys/un.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <pthread.h>

#define SOCKET_PATH "/tmp/kvstore.sock"
#define BACKLOG 10
#define BUF_SIZE 256
#define MAX_ENTRIES 100

typedef struct {
    char key[BUF_SIZE];
    char value[BUF_SIZE];
} keyvalue;

static keyvalue store[MAX_ENTRIES];
static int store_count = 0;
pthread_mutex_t store_lock = PTHREAD_MUTEX_INITIALIZER;

/* --------------------- Key-Value Store Functions --------------------- */
const char *kv_get(const char *key) {
    pthread_mutex_lock(&store_lock);
    for (int i = 0; i < store_count; i++) {
        if (strcmp(store[i].key, key) == 0) {
            pthread_mutex_unlock(&store_lock);
            return store[i].value;
        }
    }
    pthread_mutex_unlock(&store_lock);
    return NULL;
}

void kv_set(const char *key, const char *value) {
    pthread_mutex_lock(&store_lock);
    for (int i = 0; i < store_count; i++) {
        if (strcmp(store[i].key, key) == 0) {
            strncpy(store[i].value, value, BUF_SIZE - 1);
            store[i].value[BUF_SIZE - 1] = '\0';
            pthread_mutex_unlock(&store_lock);
            return;
        }
    }

    if (store_count < MAX_ENTRIES) {
        strncpy(store[store_count].key, key, BUF_SIZE - 1);
        strncpy(store[store_count].value, value, BUF_SIZE - 1);
        store[store_count].key[BUF_SIZE - 1] = '\0';
        store[store_count].value[BUF_SIZE - 1] = '\0';
        store_count++;
    }
    pthread_mutex_unlock(&store_lock);
}

/* --------------------- Error Exit --------------------- */
static void die(const char *msg) {
    perror(msg);
    unlink(SOCKET_PATH);
    exit(EXIT_FAILURE);
}

/* --------------------- Client Handler Thread --------------------- */
void *client_handler(void *arg) {
    int client_fd = *(int *)arg;
    free(arg);

    char buf[BUF_SIZE];
    ssize_t n;

    while ((n = read(client_fd, buf, sizeof(buf) - 1)) > 0) {
        buf[n] = '\0';
        char key[BUF_SIZE], value[BUF_SIZE];

        if (sscanf(buf, "SET %s %[^\n]", key, value) == 2) {
            kv_set(key, value);
            write(client_fd, "OK\n", 3);
        } else if (sscanf(buf, "GET %s", key) == 1) {
            const char *val = kv_get(key);
            if (val)
                dprintf(client_fd, "%s\n", val);
            else
                write(client_fd, "NOT_FOUND\n", 10);
        } else {
            write(client_fd, "ERROR\n", 6);
        }
    }

    close(client_fd);
    return NULL;
}

/* --------------------- Main Server --------------------- */
int main(void) {
    int listen_fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (listen_fd == -1) die("socket");

    struct sockaddr_un addr;
    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    strncpy(addr.sun_path, SOCKET_PATH, sizeof(addr.sun_path) - 1);

    unlink(SOCKET_PATH);

    if (bind(listen_fd, (struct sockaddr *)&addr, sizeof(addr)) == -1) die("bind");
    if (listen(listen_fd, BACKLOG) == -1) die("listen");

    printf("Multi-client KV Store server listening on %s\n", SOCKET_PATH);

    while (1) {
        int *client_fd = malloc(sizeof(int));
        *client_fd = accept(listen_fd, NULL, NULL);
        if (*client_fd == -1) {
            free(client_fd);
            if (errno == EINTR) continue;
            die("accept");
        }

        pthread_t tid;
        pthread_create(&tid, NULL, client_handler, client_fd);
        pthread_detach(tid); // No need to join, resources freed automatically
    }

    close(listen_fd);
    unlink(SOCKET_PATH);
    return 0;
}

