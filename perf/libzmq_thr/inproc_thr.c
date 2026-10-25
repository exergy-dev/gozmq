/* One-process PUSH->PULL throughput over inproc://, the peer (PUSH) on a
 * second thread, like libzmq's perf/inproc_thr. Output matches local_thr.c. */
#include <zmq.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/time.h>

static void *ctx;
static size_t msg_size;
static long count;

static void *pusher(void *arg) {
    (void)arg;
    void *s = zmq_socket(ctx, ZMQ_PUSH);
    if (zmq_connect(s, "inproc://thr")) { perror("connect"); exit(1); }
    for (long i = 0; i < count; i++) {
        zmq_msg_t m; zmq_msg_init_size(&m, msg_size);
        memset(zmq_msg_data(&m), 0, msg_size);
        if (zmq_msg_send(&m, s, 0) < 0) { perror("send"); exit(1); }
    }
    zmq_close(s);
    return NULL;
}

int main(int argc, char *argv[]) {
    if (argc != 3) { fprintf(stderr, "usage: inproc_thr <size> <count>\n"); return 1; }
    msg_size = atol(argv[1]);
    count = atol(argv[2]);
    ctx = zmq_ctx_new();
    void *s = zmq_socket(ctx, ZMQ_PULL);
    if (zmq_bind(s, "inproc://thr")) { perror("bind"); return 1; }
    pthread_t th;
    pthread_create(&th, NULL, pusher, NULL);
    zmq_msg_t m; zmq_msg_init(&m);
    if (zmq_msg_recv(&m, s, 0) < 0) { perror("recv"); return 1; } /* first msg: peer is up */
    struct timeval a, b; gettimeofday(&a, NULL);
    for (long i = 1; i < count; i++) {
        if (zmq_msg_recv(&m, s, 0) < 0) { perror("recv"); return 1; }
        if (zmq_msg_size(&m) != msg_size) { fprintf(stderr, "message of incorrect size received\n"); return 1; }
    }
    gettimeofday(&b, NULL);
    zmq_msg_close(&m);
    double el = (b.tv_sec - a.tv_sec) + (b.tv_usec - a.tv_usec) / 1e6;
    if (el <= 0) el = 1e-9;
    double mps = (double)(count - 1) / el;
    printf("message size: %ld [B]\nmessage count: %ld\n", (long)msg_size, count);
    printf("mean throughput: %.0f [msg/s]\n", mps);
    pthread_join(th, NULL);
    zmq_close(s); zmq_ctx_destroy(ctx);
    return 0;
}
