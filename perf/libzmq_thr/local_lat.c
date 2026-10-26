#include <zmq.h>
#include <stdio.h>
#include <stdlib.h>
int main(int argc, char *argv[]) {
    if (argc != 4) { fprintf(stderr, "usage: local_lat <bind> <size> <count>\n"); return 1; }
    const char *bind_to = argv[1];
    size_t msg_size = atol(argv[2]);
    long count = atol(argv[3]);
    void *ctx = zmq_ctx_new();
    void *s = zmq_socket(ctx, ZMQ_REP);
    if (zmq_bind(s, bind_to)) { perror("bind"); return 1; }
    zmq_msg_t m; zmq_msg_init(&m);
    for (long i = 0; i < count; i++) {
        if (zmq_msg_recv(&m, s, 0) < 0) { perror("recv"); return 1; }
        if (zmq_msg_size(&m) != msg_size) { fprintf(stderr, "message of incorrect size received\n"); return 1; }
        if (zmq_msg_send(&m, s, 0) < 0) { perror("send"); return 1; }
    }
    zmq_msg_close(&m);
    /* Linger so the final echo is delivered before teardown. */
    int linger = 5000; zmq_setsockopt(s, ZMQ_LINGER, &linger, sizeof linger);
    zmq_close(s); zmq_ctx_destroy(ctx); return 0;
}
