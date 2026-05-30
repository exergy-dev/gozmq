#include <zmq.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/time.h>
int main(int argc, char *argv[]) {
    if (argc != 4) { fprintf(stderr, "usage: local_thr <bind> <size> <count>\n"); return 1; }
    const char *bind_to = argv[1];
    size_t msg_size = atol(argv[2]);
    long count = atol(argv[3]);
    void *ctx = zmq_ctx_new();
    void *s = zmq_socket(ctx, ZMQ_PULL);
    if (zmq_bind(s, bind_to)) { perror("bind"); return 1; }
    zmq_msg_t m; zmq_msg_init(&m);
    zmq_msg_recv(&m, s, 0);                 /* first msg: connection established */
    struct timeval a, b; gettimeofday(&a, NULL);
    for (long i = 1; i < count; i++) { zmq_msg_recv(&m, s, 0); }
    gettimeofday(&b, NULL);
    zmq_msg_close(&m);
    double el = (b.tv_sec - a.tv_sec) + (b.tv_usec - a.tv_usec) / 1e6;
    double mps = (double)(count - 1) / el;
    printf("mean throughput: %.0f [msg/s]\n", mps);
    (void)msg_size;
    zmq_close(s); zmq_ctx_destroy(ctx); return 0;
}
