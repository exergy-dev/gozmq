/* One-process REQ/REP round trip over inproc://, the peer (REP, echoing) on a
 * second thread, like libzmq's perf/inproc_lat.
 *
 * CONVENTION: "average latency" below is the FULL ROUND TRIP in microseconds,
 * matching zmqperf (libzmq's own tools print one-way = round trip / 2; see
 * remote_lat.c, which prints both). */
#include <zmq.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

static void *ctx;
static size_t msg_size;
static long count;

static void *echoer(void *arg) {
    (void)arg;
    void *s = zmq_socket(ctx, ZMQ_REP);
    if (zmq_connect(s, "inproc://lat")) { perror("connect"); exit(1); }
    zmq_msg_t m; zmq_msg_init(&m);
    for (long i = 0; i < count; i++) {
        if (zmq_msg_recv(&m, s, 0) < 0) { perror("recv"); exit(1); }
        if (zmq_msg_size(&m) != msg_size) { fprintf(stderr, "message of incorrect size received\n"); exit(1); }
        if (zmq_msg_send(&m, s, 0) < 0) { perror("send"); exit(1); }
    }
    zmq_msg_close(&m);
    zmq_close(s);
    return NULL;
}

int main(int argc, char *argv[]) {
    if (argc != 3) { fprintf(stderr, "usage: inproc_lat <size> <count>\n"); return 1; }
    msg_size = atol(argv[1]);
    count = atol(argv[2]);
    ctx = zmq_ctx_new();
    void *s = zmq_socket(ctx, ZMQ_REQ);
    if (zmq_bind(s, "inproc://lat")) { perror("bind"); return 1; }
    pthread_t th;
    pthread_create(&th, NULL, echoer, NULL);
    zmq_msg_t m;
    struct timespec a, b; clock_gettime(CLOCK_MONOTONIC, &a);
    for (long i = 0; i < count; i++) {
        zmq_msg_init_size(&m, msg_size);
        memset(zmq_msg_data(&m), 0, msg_size);
        if (zmq_msg_send(&m, s, 0) < 0) { perror("send"); return 1; }
        if (zmq_msg_recv(&m, s, 0) < 0) { perror("recv"); return 1; }
        if (zmq_msg_size(&m) != msg_size) { fprintf(stderr, "message of incorrect size received\n"); return 1; }
        zmq_msg_close(&m);
    }
    clock_gettime(CLOCK_MONOTONIC, &b);
    double el_us = (b.tv_sec - a.tv_sec) * 1e6 + (b.tv_nsec - a.tv_nsec) / 1e3;
    printf("message size: %ld [B]\nroundtrip count: %ld\n", (long)msg_size, count);
    printf("average latency: %.3f [us]\n", el_us / count);
    pthread_join(th, NULL);
    zmq_close(s); zmq_ctx_destroy(ctx);
    return 0;
}
