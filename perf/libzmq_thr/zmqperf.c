/* zmqperf.c: perf/zmqperf written against libzmq, with the same commands,
 * flags, patterns and output, so compare.sh can put either library on
 * either side of every cell. Repo-written and minimal; not libzmq's own
 * perf sources.
 *
 *   zmqperf local_thr|remote_thr|local_lat|remote_lat|inproc_thr|inproc_lat
 *           [-pattern P] [-endpoint E] [-mode bind|connect] [-size N]
 *           [-count N] [-hwm N] [-prefix S] [-subs N] [-curve]
 *           [-wait D] [-idle D]
 *
 * D is a duration such as 500ms, 1s or 250us. Patterns, defaults and
 * semantics are those of perf/zmqperf (see its patterns.go); the two
 * stream patterns implement only the STREAM side here (stream_in's
 * receiver, stream_out's sender), the raw TCP peer is always the Go tool.
 *
 * Matching choices with the Go tool: every socket gets SNDHWM = RCVHWM =
 * -hwm (default 1,000,000) and infinite linger; senders send one reused
 * payload buffer with zmq_send (libzmq copies it into a message, the usual
 * C API path); receivers zmq_msg_recv into one reused zmq_msg_t; a ROUTER
 * sender sets ZMQ_ROUTER_MANDATORY so it blocks at HWM like gozmq's ROUTER
 * instead of dropping. -pool and -gogc are accepted and ignored (Go only).
 *
 * Latency is printed as the FULL ROUND TRIP ("average latency"), as the Go
 * tool does; libzmq's own remote_lat prints half of that. */
#define _GNU_SOURCE
#include <zmq.h>
#include <getopt.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

static const char *server_public = "rq:rM>}U?@Lns47E1%kR.o@n%FcmmsL/@{H8]yf7";
static const char *server_secret = "JTKVSB%%)wK0E.X)V>+}o?pNmC{O&4W4b!Ni{Lh6";
static const char *client_public = "Yne@$w-vo<fVvi]a<NY6T1ed:M$fCG*[IaLV{hID";
static const char *client_secret = "D:)Q[IlAW!ahhC2ac:9*A}h:p?([4%wOTJ%JR%cs";

typedef struct {
    const char *pattern, *endpoint, *prefix;
    size_t size;
    long count;
    int hwm, subs, curve;
    long wait_ms, idle_ms;
    char *payload;
    void *ctx;
} cfg_t;

typedef struct { long received; double elapsed; } result_t;

static void die(const char *what) {
    fprintf(stderr, "%s: %s\n", what, zmq_strerror(zmq_errno()));
    exit(1);
}

static double now(void) {
    struct timespec t;
    clock_gettime(CLOCK_MONOTONIC, &t);
    return t.tv_sec + t.tv_nsec / 1e9;
}

static long parse_duration_ms(const char *s) {
    char *end;
    double v = strtod(s, &end);
    if (!strcmp(end, "ms")) return (long)v;
    if (!strcmp(end, "s")) return (long)(v * 1000);
    if (!strcmp(end, "us")) return (long)(v / 1000);
    if (!strcmp(end, "m")) return (long)(v * 60000);
    fprintf(stderr, "bad duration %s\n", s);
    exit(1);
}

static void setopt_int(void *s, int opt, int v) {
    if (zmq_setsockopt(s, opt, &v, sizeof v)) die("setsockopt");
}

/* open creates a socket with the run's HWM, infinite linger, idle receive
 * timeout and CURVE role (the binding side is the server), then binds or
 * connects. thr_recv drops the linger to 0 afterwards: a SUB re-queues its
 * subscription for a publisher that has exited, and infinite linger would
 * then hang zmq_ctx_destroy. */
static void *open_sock(cfg_t *c, int type, int bind) {
    void *s = zmq_socket(c->ctx, type);
    if (!s) die("socket");
    setopt_int(s, ZMQ_SNDHWM, c->hwm);
    setopt_int(s, ZMQ_RCVHWM, c->hwm);
    setopt_int(s, ZMQ_LINGER, -1);
    setopt_int(s, ZMQ_RCVTIMEO, (int)c->idle_ms);
    if (c->curve) {
        if (bind) {
            setopt_int(s, ZMQ_CURVE_SERVER, 1);
            zmq_setsockopt(s, ZMQ_CURVE_SECRETKEY, server_secret, 40);
        } else {
            zmq_setsockopt(s, ZMQ_CURVE_SERVERKEY, server_public, 40);
            zmq_setsockopt(s, ZMQ_CURVE_PUBLICKEY, client_public, 40);
            zmq_setsockopt(s, ZMQ_CURVE_SECRETKEY, client_secret, 40);
        }
    }
    if (type == ZMQ_SUB) zmq_setsockopt(s, ZMQ_SUBSCRIBE, c->prefix, strlen(c->prefix));
    if (type == ZMQ_XPUB) setopt_int(s, ZMQ_XPUB_VERBOSE, 1);
    if (type == ZMQ_ROUTER) setopt_int(s, ZMQ_ROUTER_MANDATORY, 1);
    if ((bind ? zmq_bind(s, c->endpoint) : zmq_connect(s, c->endpoint)))
        die(bind ? "bind" : "connect");
    return s;
}

/* recv_whole receives every frame of one message into m (left holding the
 * last frame) and returns the frame count, or -1 on timeout. */
static int recv_whole(void *s, zmq_msg_t *m) {
    int frames = 0;
    do {
        if (zmq_msg_recv(m, s, 0) < 0) {
            if (zmq_errno() == EAGAIN) return -1;
            die("recv");
        }
        frames++;
    } while (zmq_msg_more(m));
    return frames;
}

static int recv_checked(void *s, zmq_msg_t *m, size_t size) {
    if (recv_whole(s, m) < 0) return 0;
    if (zmq_msg_size(m) != size) {
        fprintf(stderr, "message of incorrect size %zu (expected %zu)\n", zmq_msg_size(m), size);
        exit(1);
    }
    return 1;
}

static void send_or_die(void *s, const void *buf, size_t len, int flags) {
    if (zmq_send(s, buf, len, flags) < 0) die("send");
}

/* ---- throughput ------------------------------------------------------ */

static int recv_type(const char *p) {
    if (!strcmp(p, "push_pull")) return ZMQ_PULL;
    if (!strcmp(p, "pub_sub")) return ZMQ_SUB;
    if (!strcmp(p, "xpub_xsub")) return ZMQ_XSUB;
    if (!strcmp(p, "pair")) return ZMQ_PAIR;
    if (!strcmp(p, "dealer_router")) return ZMQ_ROUTER;
    if (!strcmp(p, "router_dealer")) return ZMQ_DEALER;
    if (!strcmp(p, "stream_in")) return ZMQ_STREAM;
    return -1;
}

static int send_type(const char *p) {
    if (!strcmp(p, "push_pull")) return ZMQ_PUSH;
    if (!strcmp(p, "pub_sub")) return ZMQ_PUB;
    if (!strcmp(p, "xpub_xsub")) return ZMQ_XPUB;
    if (!strcmp(p, "pair")) return ZMQ_PAIR;
    if (!strcmp(p, "dealer_router")) return ZMQ_DEALER;
    if (!strcmp(p, "router_dealer")) return ZMQ_ROUTER;
    if (!strcmp(p, "stream_out")) return ZMQ_STREAM;
    return -1;
}

/* recv_stream counts the bytes a raw TCP writer sends a STREAM socket. */
static result_t recv_stream(void *s, cfg_t *c) {
    result_t r = {0, 0};
    long long total = (long long)c->size * c->count, got = 0;
    double start = 0;
    zmq_msg_t m;
    zmq_msg_init(&m);
    while (got < total) {
        if (recv_whole(s, &m) < 0) break;
        if (zmq_msg_size(&m) == 0) continue; /* connect/disconnect notification */
        if (got == 0) start = now();
        got += zmq_msg_size(&m);
    }
    r.elapsed = now() - start;
    r.received = c->size ? (long)(got / (long long)c->size) : c->count;
    zmq_msg_close(&m);
    return r;
}

/* thr_recv: open the receiver, run its setup, receive count messages and
 * time all but the first. */
static result_t thr_recv(cfg_t *c, int bind) {
    int type = recv_type(c->pattern);
    if (type < 0) { fprintf(stderr, "pattern %s has no libzmq receiver\n", c->pattern); exit(1); }
    void *s = open_sock(c, type, bind);
    setopt_int(s, ZMQ_LINGER, 0);
    if (type == ZMQ_STREAM) { result_t r = recv_stream(s, c); zmq_close(s); return r; }
    if (type == ZMQ_XSUB) {
        size_t n = strlen(c->prefix);
        char *sub = malloc(n + 1);
        sub[0] = 1;
        memcpy(sub + 1, c->prefix, n);
        send_or_die(s, sub, n + 1, 0);
        free(sub);
    }
    if (type == ZMQ_DEALER) send_or_die(s, "hello", 5, 0);
    result_t r = {0, 0};
    zmq_msg_t m;
    zmq_msg_init(&m);
    if (recv_checked(s, &m, c->size)) {
        r.received = 1;
        double a = now();
        while (r.received < c->count && recv_checked(s, &m, c->size)) r.received++;
        r.elapsed = now() - a;
    }
    zmq_msg_close(&m);
    zmq_close(s);
    return r;
}

/* thr_send: open the sender, run its setup, send count messages. */
static void thr_send(cfg_t *c, int bind) {
    int type = send_type(c->pattern);
    if (type < 0) { fprintf(stderr, "pattern %s has no libzmq sender\n", c->pattern); exit(1); }
    void *s = open_sock(c, type, bind);
    zmq_msg_t m;
    zmq_msg_init(&m);
    char id[256];
    size_t idlen = 0;
    if (type == ZMQ_PUB) usleep(c->wait_ms * 1000);
    if (type == ZMQ_XPUB)
        for (int i = 0; i < c->subs; i++)
            if (recv_whole(s, &m) < 0) die("xpub recv subscription");
    if (type == ZMQ_ROUTER || type == ZMQ_STREAM) {
        /* ROUTER: the DEALER's hello. STREAM: skip the empty connect
         * notification, then the raw peer's "go". The first frame is the
         * peer's identity either way. */
        setopt_int(s, ZMQ_RCVTIMEO, -1);
        for (;;) {
            if (zmq_msg_recv(&m, s, 0) < 0) die("recv identity");
            idlen = zmq_msg_size(&m);
            if (idlen > sizeof id) { fprintf(stderr, "identity too long\n"); exit(1); }
            memcpy(id, zmq_msg_data(&m), idlen);
            if (zmq_msg_recv(&m, s, 0) < 0) die("recv body");
            if (type == ZMQ_ROUTER || zmq_msg_size(&m) > 0) break;
        }
    }
    for (long i = 0; i < c->count; i++) {
        if (idlen) send_or_die(s, id, idlen, ZMQ_SNDMORE);
        send_or_die(s, c->payload, c->size, 0);
    }
    zmq_msg_close(&m);
    zmq_close(s);
}

/* ---- latency --------------------------------------------------------- */

static int server_type(const char *p) {
    if (!strcmp(p, "req_rep") || !strcmp(p, "dealer_rep")) return ZMQ_REP;
    if (!strcmp(p, "req_router") || !strcmp(p, "dealer_router")) return ZMQ_ROUTER;
    if (!strcmp(p, "pair")) return ZMQ_PAIR;
    return -1;
}

static int client_type(const char *p) {
    if (!strcmp(p, "req_rep") || !strcmp(p, "req_router")) return ZMQ_REQ;
    if (!strcmp(p, "dealer_rep") || !strcmp(p, "dealer_router")) return ZMQ_DEALER;
    if (!strcmp(p, "pair")) return ZMQ_PAIR;
    return -1;
}

/* lat_server echoes count messages, forwarding each frame as it arrives
 * (a ROUTER's identity frame routes the echo). */
static void lat_server(cfg_t *c, int bind) {
    int type = server_type(c->pattern);
    if (type < 0) { fprintf(stderr, "unknown latency pattern %s\n", c->pattern); exit(1); }
    void *s = open_sock(c, type, bind);
    setopt_int(s, ZMQ_RCVTIMEO, -1);
    zmq_msg_t m;
    zmq_msg_init(&m);
    for (long i = 0; i < c->count; i++) {
        int more;
        do {
            if (zmq_msg_recv(&m, s, 0) < 0) die("recv");
            more = zmq_msg_more(&m);
            if (zmq_msg_send(&m, s, more ? ZMQ_SNDMORE : 0) < 0) die("send");
        } while (more);
    }
    zmq_msg_close(&m);
    zmq_close(s);
}

static double lat_client(cfg_t *c, int bind) {
    int type = client_type(c->pattern);
    if (type < 0) { fprintf(stderr, "unknown latency pattern %s\n", c->pattern); exit(1); }
    void *s = open_sock(c, type, bind);
    setopt_int(s, ZMQ_RCVTIMEO, -1);
    int envelope = !strcmp(c->pattern, "dealer_rep");
    zmq_msg_t m;
    zmq_msg_init(&m);
    double a = now();
    for (long i = 0; i < c->count; i++) {
        if (envelope) send_or_die(s, "", 0, ZMQ_SNDMORE);
        send_or_die(s, c->payload, c->size, 0);
        if (!recv_checked(s, &m, c->size)) die("recv");
    }
    double el = now() - a;
    zmq_msg_close(&m);
    zmq_close(s);
    return el;
}

/* ---- reporting and inproc --------------------------------------------- */

static int report_thr(cfg_t *c, result_t r) {
    if (r.received < c->count) {
        printf("received: %ld of %ld\n", r.received, c->count);
        fflush(stdout);
        return 1;
    }
    if (r.elapsed <= 0) r.elapsed = 1e-9;
    double mps = (double)(c->count - 1) / r.elapsed;
    printf("received: %ld\n", r.received);
    printf("mean throughput: %.0f [msg/s]\n", mps);
    printf("mean throughput: %.3f [Mb/s]\n", mps * c->size * 8 / 1e6);
    fflush(stdout);
    return 0;
}

static void report_lat(cfg_t *c, double el) {
    printf("average latency: %.3f [us]\n", el * 1e6 / c->count);
    printf("roundtrip rate: %.0f [msg/s]\n", c->count / el);
    fflush(stdout);
}

typedef struct { cfg_t *c; int bind; result_t r; } job_t;

static void *recv_job(void *a) { job_t *j = a; j->r = thr_recv(j->c, j->bind); return NULL; }
static void *send_job(void *a) { job_t *j = a; thr_send(j->c, j->bind); return NULL; }
static void *server_job(void *a) { job_t *j = a; lat_server(j->c, j->bind); return NULL; }

int main(int argc, char **argv) {
    if (argc < 2) { fprintf(stderr, "usage: zmqperf <command> [flags]\n"); return 1; }
    const char *cmd = argv[1];
    int local = !strcmp(cmd, "local_thr") || !strcmp(cmd, "local_lat");
    int thr = strstr(cmd, "_thr") != NULL;
    cfg_t c = {0};
    c.pattern = thr ? "push_pull" : "req_rep";
    c.endpoint = local ? "tcp://*:5555" : "tcp://127.0.0.1:5555";
    c.prefix = "";
    c.size = 32;
    c.hwm = 1000000;
    c.subs = 1;
    c.wait_ms = 500;
    c.idle_ms = 5000;
    const char *mode = "";
    static struct option opts[] = {
        {"pattern", 1, 0, 'p'}, {"endpoint", 1, 0, 'e'}, {"mode", 1, 0, 'm'},
        {"size", 1, 0, 's'}, {"count", 1, 0, 'n'}, {"hwm", 1, 0, 'h'},
        {"prefix", 1, 0, 'x'}, {"subs", 1, 0, 'u'}, {"curve", 0, 0, 'c'},
        {"wait", 1, 0, 'w'}, {"idle", 1, 0, 'i'},
        {"pool", 2, 0, 'P'}, {"gogc", 1, 0, 'G'}, {0, 0, 0, 0}};
    optind = 2;
    int o;
    while ((o = getopt_long_only(argc, argv, "", opts, NULL)) != -1) {
        switch (o) {
        case 'p': c.pattern = optarg; break;
        case 'e': c.endpoint = optarg; break;
        case 'm': mode = optarg; break;
        case 's': c.size = atol(optarg); break;
        case 'n': c.count = atol(optarg); break;
        case 'h': c.hwm = atoi(optarg); break;
        case 'x': c.prefix = optarg; break;
        case 'u': c.subs = atoi(optarg); break;
        case 'c': c.curve = 1; break;
        case 'w': c.wait_ms = parse_duration_ms(optarg); break;
        case 'i': c.idle_ms = parse_duration_ms(optarg); break;
        case 'P': case 'G': break;
        default: return 1;
        }
    }
    if (c.count == 0) c.count = thr ? 1000000 : 100000;
    int bind = local;
    if (!strcmp(mode, "bind")) bind = 1;
    else if (!strcmp(mode, "connect")) bind = 0;
    else if (*mode) { fprintf(stderr, "-mode must be bind or connect\n"); return 1; }
    c.payload = malloc(c.size ? c.size : 1);
    memset(c.payload, 'a', c.size);
    c.ctx = zmq_ctx_new();

    int rc = 0;
    printf("message size: %zu [B]\n%s count: %ld\n", c.size, thr ? "message" : "roundtrip", c.count);
    fflush(stdout);
    if (!strcmp(cmd, "local_thr")) {
        rc = report_thr(&c, thr_recv(&c, bind));
    } else if (!strcmp(cmd, "remote_thr")) {
        thr_send(&c, bind);
    } else if (!strcmp(cmd, "local_lat")) {
        lat_server(&c, bind);
    } else if (!strcmp(cmd, "remote_lat")) {
        report_lat(&c, lat_client(&c, bind));
    } else if (!strcmp(cmd, "inproc_thr")) {
        c.endpoint = "inproc://zmqperf-thr";
        if (c.wait_ms > 100) c.wait_ms = 100;
        int n = c.subs;
        job_t sender = {&c, n > 1, {0, 0}};
        job_t *recv = calloc(n, sizeof *recv);
        pthread_t st, *rt = calloc(n, sizeof *rt);
        if (n > 1) { /* fan-out: the sender binds, every receiver connects */
            pthread_create(&st, NULL, send_job, &sender);
            usleep(10000);
            for (int i = 0; i < n; i++) { recv[i] = (job_t){&c, 0, {0, 0}}; pthread_create(&rt[i], NULL, recv_job, &recv[i]); }
        } else { /* the receiver binds before the sender connects */
            recv[0] = (job_t){&c, 1, {0, 0}};
            pthread_create(&rt[0], NULL, recv_job, &recv[0]);
            usleep(10000);
            pthread_create(&st, NULL, send_job, &sender);
        }
        result_t worst = {c.count + 1, 0};
        for (int i = 0; i < n; i++) {
            pthread_join(rt[i], NULL);
            result_t r = recv[i].r;
            if (r.received < worst.received || (r.received == worst.received && r.elapsed > worst.elapsed)) worst = r;
        }
        pthread_join(st, NULL);
        rc = report_thr(&c, worst);
    } else if (!strcmp(cmd, "inproc_lat")) {
        c.endpoint = "inproc://zmqperf-lat";
        job_t server = {&c, 1, {0, 0}};
        pthread_t th;
        pthread_create(&th, NULL, server_job, &server);
        usleep(10000);
        report_lat(&c, lat_client(&c, 0));
        pthread_join(th, NULL);
    } else {
        fprintf(stderr, "unknown command %s\n", cmd);
        return 1;
    }
    zmq_ctx_destroy(c.ctx);
    return rc;
}
