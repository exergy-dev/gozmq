#include <zmq.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
int main(int argc, char *argv[]) {
    if (argc != 4) { fprintf(stderr, "usage: remote_thr <connect> <size> <count>\n"); return 1; }
    const char *connect_to = argv[1];
    size_t msg_size = atol(argv[2]);
    long count = atol(argv[3]);
    void *ctx = zmq_ctx_new();
    void *s = zmq_socket(ctx, ZMQ_PUSH);
    if (zmq_connect(s, connect_to)) { perror("connect"); return 1; }
    for (long i = 0; i < count; i++) {
        zmq_msg_t m; zmq_msg_init_size(&m, msg_size);
        memset(zmq_msg_data(&m), 0, msg_size);
        zmq_msg_send(&m, s, 0);
        zmq_msg_close(&m);
    }
    zmq_close(s); zmq_ctx_destroy(ctx); return 0;
}
