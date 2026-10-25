#include <zmq.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
int main(int argc, char *argv[]) {
    if (argc != 4) { fprintf(stderr, "usage: remote_lat <connect> <size> <count>\n"); return 1; }
    const char *connect_to = argv[1];
    size_t msg_size = atol(argv[2]);
    long count = atol(argv[3]);
    void *ctx = zmq_ctx_new();
    void *s = zmq_socket(ctx, ZMQ_REQ);
    if (zmq_connect(s, connect_to)) { perror("connect"); return 1; }
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
    printf("message size: %ld [B]\n", (long)msg_size);
    printf("roundtrip count: %ld\n", count);
    /* libzmq convention: "average latency" is ONE-WAY (round trip / 2). */
    printf("average latency: %.3f [us]\n", el_us / count / 2);
    /* Full round trip, so scripts never have to guess the convention. */
    printf("roundtrip latency: %.3f [us]\n", el_us / count);
    zmq_close(s); zmq_ctx_destroy(ctx); return 0;
}
