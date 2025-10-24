// kvstore_txn.c
// Compile: gcc -O2 -pthread kvstore_txn.c -o kvstore_txn
// Run: ./kvstore_txn
//
// Simple in-memory key-value store with transactions, per-key exclusive locks,
// wait-for graph based deadlock detection and victim selection (youngest txn).

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <unistd.h>
#include <stdint.h>
#include <stdbool.h>
#include <time.h>

#define MAX_KEYS 128
#define MAX_TXNS 32
#define KEYLEN 64
#define MAX_WRITES 64

/* ---------- KV store (simple array buckets) ---------- */
typedef struct KVItem {
    char key[KEYLEN];
    char *value;
    struct KVItem *next;
} KVItem;

typedef struct {
    KVItem *buckets[MAX_KEYS];
    pthread_mutex_t mtx; // protects store structure
} KVStore;

/* ---------- Per-key lock ---------- */
typedef struct {
    int holder; // txn id or -1
    pthread_mutex_t mtx;
    pthread_cond_t cond;
} KeyLock;

/* ---------- Transaction ---------- */
typedef struct {
    int id;               // 0..MAX_TXNS-1
    uint64_t start_seq;   // increasing sequence for victim selection
    volatile bool aborted;
    KeyLock *held_locks[MAX_KEYS];
    int held_count;
    // local write set (applied at commit)
    struct {
        char key[KEYLEN];
        char *value;
    } write_set[MAX_WRITES];
    int write_cnt;
} Transaction;

/* ---------- Globals ---------- */
static KVStore gkv;
static KeyLock glocks[MAX_KEYS];

static Transaction *txns[MAX_TXNS]; // slot -> Transaction*
static uint64_t seq_counter = 0;
static int txn_slots_used = 0;
static pthread_mutex_t txn_mtx = PTHREAD_MUTEX_INITIALIZER;

/* wait-for graph: wait_for[a][b] == true means txn a waits for txn b */
static bool wait_for[MAX_TXNS][MAX_TXNS];
static pthread_mutex_t wf_mtx = PTHREAD_MUTEX_INITIALIZER;

/* ---------- Utilities ---------- */
static unsigned hash_key(const char *k){
    uint32_t h = 2166136261u;
    const unsigned char *p = (const unsigned char*)k;
    while(*p){
        h ^= *p++;
        h *= 16777619u;
    }
    return h % MAX_KEYS;
}

/* ---------- KV store functions ---------- */
static void kv_init(KVStore *s){
    for(int i=0;i<MAX_KEYS;i++) s->buckets[i] = NULL;
    pthread_mutex_init(&s->mtx, NULL);
}

static char *kv_read(KVStore *s, const char *key){
    unsigned idx = hash_key(key);
    pthread_mutex_lock(&s->mtx);
    KVItem *it = s->buckets[idx];
    while(it){
        if(strcmp(it->key, key) == 0){
            char *val = it->value ? strdup(it->value) : NULL;
            pthread_mutex_unlock(&s->mtx);
            return val;
        }
        it = it->next;
    }
    pthread_mutex_unlock(&s->mtx);
    return NULL;
}

static void kv_write(KVStore *s, const char *key, const char *value){
    unsigned idx = hash_key(key);
    pthread_mutex_lock(&s->mtx);
    KVItem *it = s->buckets[idx];
    while(it){
        if(strcmp(it->key, key) == 0){
            free(it->value);
            it->value = value ? strdup(value) : NULL;
            pthread_mutex_unlock(&s->mtx);
            return;
        }
        it = it->next;
    }
    // insert new
    KVItem *n = malloc(sizeof(KVItem));
    strncpy(n->key, key, KEYLEN-1);
    n->key[KEYLEN-1] = '\0';
    n->value = value ? strdup(value) : NULL;
    n->next = s->buckets[idx];
    s->buckets[idx] = n;
    pthread_mutex_unlock(&s->mtx);
}

/* ---------- Lock initialization ---------- */
static void locks_init(void){
    for(int i=0;i<MAX_KEYS;i++){
        glocks[i].holder = -1;
        pthread_mutex_init(&glocks[i].mtx, NULL);
        pthread_cond_init(&glocks[i].cond, NULL);
    }
}

/* ---------- Wait-for graph helpers ---------- */
static void wf_add_edge(int a, int b){
    if(a<0 || b<0 || a>=MAX_TXNS || b>=MAX_TXNS) return;
    wait_for[a][b] = true;
}
static void wf_remove_edge(int a, int b){
    if(a<0 || b<0 || a>=MAX_TXNS || b>=MAX_TXNS) return;
    wait_for[a][b] = false;
}
static void wf_clear_outgoing(int a){
    if(a<0 || a>=MAX_TXNS) return;
    for(int j=0;j<MAX_TXNS;j++) wait_for[a][j] = false;
}
static void wf_remove_incoming_to(int b){
    if(b<0 || b>=MAX_TXNS) return;
    for(int i=0;i<MAX_TXNS;i++) wait_for[i][b] = false;
}

/* ---------- Deadlock detection with victim selection ---------- */

/* DFS helper to find a cycle and collect nodes in the cycle; returns true if a cycle found.
   On finding a cycle, it sets *victim to the txn id (with highest start_seq) inside cycle. */
static bool dfs_cycle(int u, bool visited[], bool stack[], int parent[], int *victim_out){
    visited[u] = true;
    stack[u] = true;
    for(int v=0; v<MAX_TXNS; v++){
        if(!wait_for[u][v]) continue;
        if(!visited[v]){
            parent[v] = u;
            if(dfs_cycle(v, visited, stack, parent, victim_out)) return true;
        } else if(stack[v]){
            // found a cycle from v .. u
            // collect cycle nodes via parent pointers
            int cur = u;
            int cycle_nodes[MAX_TXNS];
            int cnt = 0;
            cycle_nodes[cnt++] = v;
            while(cur != v && cur != -1){
                cycle_nodes[cnt++] = cur;
                cur = parent[cur];
            }
            // choose victim: txn with max start_seq
            int victim = -1;
            uint64_t max_seq = 0;
            for(int i=0;i<cnt;i++){
                int t = cycle_nodes[i];
                if(t>=0 && t<MAX_TXNS && txns[t] != NULL){
                    if(txns[t]->start_seq > max_seq){
                        max_seq = txns[t]->start_seq;
                        victim = t;
                    }
                }
            }
            if(victim_out) *victim_out = victim;
            return true;
        }
    }
    stack[u] = false;
    return false;
}

static bool detect_cycle_and_select_victim(int *victim_out){
    bool visited[MAX_TXNS] = {0};
    bool stack[MAX_TXNS] = {0};
    int parent[MAX_TXNS];
    for(int i=0;i<MAX_TXNS;i++) parent[i] = -1;

    for(int s=0; s<MAX_TXNS; s++){
        if(txns[s] == NULL) continue;
        if(!visited[s]){
            if(dfs_cycle(s, visited, stack, parent, victim_out)){
                return true;
            }
        }
    }
    return false;
}

/* ---------- Acquire lock (with wait-for graph & deadlock detection) ---------- */
static int acquire_lock_txn(Transaction *t, const char *key){
    if(t->aborted) return -1;
    unsigned idx = hash_key(key);
    KeyLock *lk = &glocks[idx];

    pthread_mutex_lock(&lk->mtx);
    // fast path: free or already held by this txn
    if(lk->holder == -1 || lk->holder == t->id){
        lk->holder = t->id;
        // record held lock (if not already)
        bool found=false;
        for(int i=0;i<t->held_count;i++) if(t->held_locks[i]==lk) { found=true; break;}
        if(!found){
            t->held_locks[t->held_count++] = lk;
        }
        pthread_mutex_unlock(&lk->mtx);
        // clear outgoing wait edges for t (it now holds a resource)
        pthread_mutex_lock(&wf_mtx);
        wf_clear_outgoing(t->id);
        pthread_mutex_unlock(&wf_mtx);
        return 0;
    }

    // otherwise need to wait; add wait-for edge t -> holder
    pthread_mutex_lock(&wf_mtx);
    wf_add_edge(t->id, lk->holder);

    // detect cycle and select victim if any
    int victim = -1;
    if(detect_cycle_and_select_victim(&victim)){
        if(victim >= 0 && txns[victim] != NULL){
            // mark victim aborted
            txns[victim]->aborted = true;
            // we do NOT immediately remove edges here; victim will cleanup when it wakes/observes aborted
            fprintf(stderr, "[DEADLOCK] victim chosen txn=%d (seq=%lu)\n",
                    txns[victim]->id, (unsigned long)txns[victim]->start_seq);
        }
    }
    pthread_mutex_unlock(&wf_mtx);

    // wait until lock is free or this txn becomes aborted
    while(!t->aborted){
        if(lk->holder == -1){
            // acquire
            lk->holder = t->id;
            // record held lock
            bool found=false;
            for(int i=0;i<t->held_count;i++) if(t->held_locks[i]==lk) { found=true; break;}
            if(!found){
                t->held_locks[t->held_count++] = lk;
            }
            // clear outgoing edges for this txn
            pthread_mutex_lock(&wf_mtx);
            wf_clear_outgoing(t->id);
            pthread_mutex_unlock(&wf_mtx);
            pthread_mutex_unlock(&lk->mtx);
            return 0;
        }
        // timed wait so that we periodically re-check aborted flag & potential changes
        struct timespec ts;
        clock_gettime(CLOCK_REALTIME, &ts);
        ts.tv_nsec += 200 * 1000000; // 200 ms
        if(ts.tv_nsec >= 1000000000){ ts.tv_sec += 1; ts.tv_nsec -= 1000000000; }
        pthread_cond_timedwait(&lk->cond, &lk->mtx, &ts);
        // loop will re-check
    }

    // aborted: cleanup outgoing edges
    pthread_mutex_lock(&wf_mtx);
    wf_clear_outgoing(t->id);
    pthread_mutex_unlock(&wf_mtx);

    pthread_mutex_unlock(&lk->mtx);
    return -1;
}

/* ---------- Release all locks held by transaction ---------- */
static void release_all_locks(Transaction *t){
    for(int i=0;i<t->held_count;i++){
        KeyLock *lk = t->held_locks[i];
        pthread_mutex_lock(&lk->mtx);
        if(lk->holder == t->id) lk->holder = -1;
        // remove incoming edges to this txn (others waiting on it)
        pthread_mutex_lock(&wf_mtx);
        wf_remove_incoming_to(t->id);
        pthread_mutex_unlock(&wf_mtx);
        pthread_cond_broadcast(&lk->cond);
        pthread_mutex_unlock(&lk->mtx);
    }
    t->held_count = 0;
}

/* ---------- Transaction lifecycle ---------- */
static Transaction* txn_begin(void){
    pthread_mutex_lock(&txn_mtx);
    int slot = -1;
    for(int i=0;i<MAX_TXNS;i++){
        if(txns[i] == NULL){
            slot = i;
            break;
        }
    }
    if(slot == -1){
        pthread_mutex_unlock(&txn_mtx);
        return NULL;
    }
    Transaction *t = calloc(1, sizeof(Transaction));
    t->id = slot;
    t->start_seq = ++seq_counter;
    t->aborted = false;
    t->held_count = 0;
    t->write_cnt = 0;
    txns[slot] = t;
    pthread_mutex_unlock(&txn_mtx);
    return t;
}

static void txn_free(Transaction *t){
    if(!t) return;
    // free local buffered writes
    for(int i=0;i<t->write_cnt;i++){
        free(t->write_set[i].value);
    }
    pthread_mutex_lock(&txn_mtx);
    if(t->id >= 0 && t->id < MAX_TXNS && txns[t->id] == t) txns[t->id] = NULL;
    pthread_mutex_unlock(&txn_mtx);
    free(t);
}

/* ---------- Transactional operations ---------- */
static int txn_get(Transaction *t, const char *key, char **out_val){
    if(t->aborted) return -1;
    // if present in write set return latest
    for(int i=0;i<t->write_cnt;i++){
        if(strcmp(t->write_set[i].key, key) == 0){
            *out_val = strdup(t->write_set[i].value);
            return 0;
        }
    }
    if(acquire_lock_txn(t, key) < 0) return -1;
    char *v = kv_read(&gkv, key);
    *out_val = v;
    return 0;
}

static int txn_put(Transaction *t, const char *key, const char *value){
    if(t->aborted) return -1;
    if(acquire_lock_txn(t, key) < 0) return -1;
    // buffer the write
    if(t->write_cnt >= MAX_WRITES) return -1;
    strncpy(t->write_set[t->write_cnt].key, key, KEYLEN-1);
    t->write_set[t->write_cnt].key[KEYLEN-1]=0;
    t->write_set[t->write_cnt].value = strdup(value);
    t->write_cnt++;
    return 0;
}

static int txn_commit(Transaction *t){
    if(t->aborted){
        release_all_locks(t);
        txn_free(t);
        return -1;
    }
    // apply buffered writes atomically (we serialize on store lock inside kv_write)
    for(int i=0;i<t->write_cnt;i++){
        kv_write(&gkv, t->write_set[i].key, t->write_set[i].value);
    }
    // clear outgoing edges and release locks
    pthread_mutex_lock(&wf_mtx);
    wf_clear_outgoing(t->id);
    pthread_mutex_unlock(&wf_mtx);

    release_all_locks(t);
    txn_free(t);
    return 0;
}

static void txn_abort(Transaction *t){
    t->aborted = true;
    pthread_mutex_lock(&wf_mtx);
    wf_clear_outgoing(t->id);
    pthread_mutex_unlock(&wf_mtx);
    release_all_locks(t);
    txn_free(t);
}

/* ---------- Demo threads (classic deadlock) ---------- */
void *thread1_fn(void *arg){
    Transaction *t = txn_begin();
    if(!t){ fprintf(stderr, "txn begin failed T1\n"); return NULL; }
    printf("T1 id=%d seq=%lu begin\n", t->id, (unsigned long)t->start_seq);
    char *v;
    if(txn_get(t, "x", &v) == 0){
        printf("T1 read x=%s\n", v?v:"(null)");
        free(v);
    } else {
        printf("T1 get x failed (aborted?)\n");
        return NULL;
    }
    sleep(1); // hold x for a bit
    printf("T1 trying to put y=100\n");
    if(txn_put(t, "y", "100") < 0){
        printf("T1 put y failed (aborted?)\n");
        txn_abort(t);
        return NULL;
    }
    if(txn_commit(t) == 0) printf("T1 committed\n");
    else printf("T1 commit failed (aborted)\n");
    return NULL;
}

void *thread2_fn(void *arg){
    Transaction *t = txn_begin();
    if(!t){ fprintf(stderr, "txn begin failed T2\n"); return NULL; }
    printf("T2 id=%d seq=%lu begin\n", t->id, (unsigned long)t->start_seq);
    char *v;
    if(txn_get(t, "y", &v) == 0){
        printf("T2 read y=%s\n", v?v:"(null)");
        free(v);
    } else {
        printf("T2 get y failed (aborted?)\n");
        return NULL;
    }
    sleep(1);
    printf("T2 trying to put x=200\n");
    if(txn_put(t, "x", "200") < 0){
        printf("T2 put x failed (aborted?)\n");
        txn_abort(t);
        return NULL;
    }
    if(txn_commit(t) == 0) printf("T2 committed\n");
    else printf("T2 commit failed (aborted)\n");
    return NULL;
}

/* ---------- main ---------- */
int main(void){
    kv_init(&gkv);
    locks_init();
    for(int i=0;i<MAX_TXNS;i++) for(int j=0;j<MAX_TXNS;j++) wait_for[i][j]=false;
    for(int i=0;i<MAX_TXNS;i++) txns[i] = NULL;

    // seed keys
    kv_write(&gkv, "x", "1");
    kv_write(&gkv, "y", "2");

    pthread_t t1, t2;
    pthread_create(&t1, NULL, thread1_fn, NULL);
    usleep(200000); // small stagger
    pthread_create(&t2, NULL, thread2_fn, NULL);

    pthread_join(t1, NULL);
    pthread_join(t2, NULL);

    char *vx = kv_read(&gkv, "x");
    char *vy = kv_read(&gkv, "y");
    printf("Final: x=%s y=%s\n", vx?vx:"(null)", vy?vy:"(null)");
    free(vx); free(vy);

    return 0;
}

