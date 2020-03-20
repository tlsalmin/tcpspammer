#include <unistd.h>
#include <stdlib.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <assert.h>
#include <limits.h>
#include <string.h>
#include <stdio.h>
#include <sched.h>
#include <unistd.h>
#include <fcntl.h>
#include <stdbool.h>
#include <pthread.h>
#include <stdarg.h>
#include <signal.h>
#include <netdb.h>
#include <errno.h>
#include <sys/sysinfo.h>
#include <sys/timerfd.h>
#include <sys/eventfd.h>
#include <sys/signalfd.h>
#include <sys/resource.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/epoll.h>

int logpipe[2];
static int verbosity = 0;
static unsigned int keepidle_value = 5;
static unsigned int max_conns_per_loop = 8192;

static void log_func(int lvl, const char *func, unsigned int line,
                    const char *fmt, ...)__attribute__((format(printf, 4, 5)));

static void log_func(int lvl, const char *func, unsigned int line,
                    const char *fmt, ...)
{
  if (lvl <= verbosity)
    {
      va_list ap;
      char buf[BUFSIZ];
      size_t len;
      ssize_t ret;

      len = snprintf(buf, sizeof(buf), "%s:%u (#%d) ", func, line,
                     sched_getcpu());
      va_start(ap, fmt);
      len += vsnprintf(buf + len, sizeof(buf) - len, fmt, ap);
      va_end(ap);


      ret = write(logpipe[1], buf, len);
      if (ret < 0 || ret != (ssize_t)len)
        {
          if (errno != EINTR && errno != EAGAIN && errno != EWOULDBLOCK)
            {
              perror("Writing to log pipe");
            }
        }
    }
}

#define LOG(_lvl, _fmt, ...)                                                   \
  log_func(_lvl, __func__, __LINE__, _fmt "\n", ##__VA_ARGS__)
#define FATAL(...) LOG(-1, __VA_ARGS__)
#define ERR(...) LOG(0, __VA_ARGS__)
#define INF(...) LOG(1, __VA_ARGS__)
#define DBG(...) LOG(2, __VA_ARGS__)

/** @brief Context per thread. */
struct tcpspammer_thread_ctx
{
  int efd;         //!< epoll fd containing the other event fds.
  int efd_connect; //!< fd for connected sockets.
  int efd_extra;   //!< event fd for listening connections.
  int efd_echo;    //!< Event fd for echo request.
  int timer;       //!< event fd for listening connections.
  int cpu;         //!< Cpu this thread is running on.
  _Atomic uint64_t active_connections;      //!< Connections started.
  _Atomic uint64_t established_connections; //!< Connections handshaked.
  pthread_t thread;
  pthread_attr_t attr;
  struct tcpspammer_ctx *main_ctx;
};

/** @brief Main context */
struct tcpspammer_ctx
{
  unsigned long n_threads; //!< Number of threads created.
  int signalfd;            //!< Main thread signal fd.
  int eventfd;             //!< Eventfd for signaling program exit.
  int efd;                 //!< Main thread epoll fd.
  int timerfd;             //!< timer fd for printing stats.
  struct tcpspammer_thread_ctx *threads; //!< All threads.
  struct sockaddr_storage *sources;      //!< Array of source addresses.
  struct sockaddr_storage *destinations; //!< Array of destination addresses
  struct timespec echo_timer;            //!< If echo set, timer for echo.
  char *echostring;                      //!< String to echo.
  unsigned int n_sources;                //!< Number of available sources.
  unsigned int n_destinations;           //!< Number of destinations.
  unsigned int n_connections;            //!< Number of connections per thread.
  unsigned int n_connections_per_sec;    //!< Number of connections per second.
  _Atomic unsigned short n_finished;     //!< Number of threads finished.
  _Atomic uint64_t new_connections;      //!< New connections this second.
  bool listen; //!< True if process listens rather than connects.
  bool disconnect_after_hello; //!< True if connection is closed after reply.
  bool explicit_echo; //!< Echo timer explicitly set.
};

__thread char ipstr_src[INET6_ADDRSTRLEN];
__thread char ipstr_dst[INET6_ADDRSTRLEN];
__thread char service_str_src[8];
__thread char service_str_dst[8];

static void strings_set(struct sockaddr_storage *saddr, bool dst,
                        int lvl)
{
  if (lvl <= verbosity)
    {
      int ret = getnameinfo((struct sockaddr *)saddr, sizeof(*saddr),
                            (dst) ? ipstr_dst : ipstr_src, INET6_ADDRSTRLEN,
                            (dst) ? service_str_dst : service_str_src, 8,
                            NI_NUMERICHOST | NI_NUMERICSERV);
      if (ret)
        {
          ERR("Failed to solve address to string: %s", gai_strerror(ret));
        }
    }
}

__attribute__((constructor)) void init_logpipe(void)
{
  if (pipe2(logpipe, O_CLOEXEC | O_NONBLOCK))
    {
      abort();
    }
}

static void safe_close(int *efd)
{
  if (*efd != -1)
    {
      if (close(*efd) == -1 && errno == EBADF)
        {
          abort();
        }
      *efd = -1;
    }
}

static void safe_close_remove(int *fd, int efd)
{
  if (*fd != -1)
    {
      if (epoll_ctl(efd, EPOLL_CTL_DEL, *fd, NULL))
        {
          FATAL("Failed to remove %d from %d: %m", *fd, efd);
        }
      safe_close(fd);
    }
}

static void safe_close_decrement(int *fd, int efd,
                                 struct tcpspammer_thread_ctx *ctx,
                                 bool was_active)
{
  ctx->active_connections--;
  if (was_active)
    {
      ctx->established_connections--;
      if (ctx->efd_echo != -1)
        {
          if (efd != ctx->efd_echo &&
              epoll_ctl(ctx->efd_echo, EPOLL_CTL_DEL, *fd, NULL))
            {
              ERR("Failed to remove %d from echo efd %d: %m", *fd,
                  ctx->efd_echo);
            }
        }
    }
  safe_close_remove(fd, efd);
}

static int read_timer(int fd)
{
  uint64_t val;

  if (read(fd, &val, sizeof(val)) > 0)
    {
      return val;
    }
  else
    {
      ERR("Failed to read timer %d: %m", fd);
    }
  return -1;
}

static int init_timer(struct timespec tv)
{
  int fd = timerfd_create(CLOCK_MONOTONIC, TFD_NONBLOCK | TFD_CLOEXEC);

  if (fd != -1)
    {
      struct itimerspec interval = { tv, tv };

      if (!timerfd_settime(fd, 0, &interval, NULL))
        {
          return fd;
        }
      else
        {
          FATAL("Failed to set timer %d: %m", fd);
        }
      safe_close(&fd);
    }
  else
    {
      FATAL("Failed to create timerfd: %m");
    }
  return fd;
}

static int signalfd_init(void)
{
  sigset_t sigs;

  sigemptyset(&sigs);
  sigaddset(&sigs, SIGINT);
  sigaddset(&sigs, SIGTERM);
  sigaddset(&sigs, SIGPIPE);
  if (!sigprocmask(SIG_BLOCK, &sigs, NULL))
    {
      int sfd = signalfd(-1, &sigs, SFD_CLOEXEC | SFD_NONBLOCK);

      if (sfd != -1)
        {
          return sfd;
        }
      else
        {
          ERR("Failed to create signalfd: %m");
        }
    }
  else
    {
      ERR("Failed to block sigs: %m");
    }
  return -1;
}

static bool max_nofile_soft(void)
{
  struct rlimit limits = {};

  if (!getrlimit(RLIMIT_NOFILE, &limits))
    {
      INF("Increasing soft limit from %lu to %lu", limits.rlim_cur,
          limits.rlim_max);

      limits.rlim_cur = limits.rlim_max;
      if (!setrlimit(RLIMIT_NOFILE, &limits))
        {
          return true;
        }
      else
        {
          FATAL("Failed to increase soft limit to %lu: %m", limits.rlim_cur);
        }
    }
  else
    {
      FATAL("Failed to get soft limits: %m");
    }
  return false;
}

static bool max_nofile(void)
{
  unsigned long maxes[] = {UINT_MAX, UINT_MAX};
  char *files[] = {"/proc/sys/fs/file-max", "/proc/sys/fs/nr_open"};
  unsigned int i;
  struct rlimit limits = {};

  for (i = 0; i < 2; i++)
    {
      FILE *fp = fopen(files[i], "r");

      if (fp)
        {
          char buf[BUFSIZ];
          size_t n_read;

          n_read = fread(buf, 1, sizeof(buf), fp);
          if (n_read > 0 && n_read < sizeof(buf))
            {
              buf[n_read] = '\0';
              maxes[i] = atoll(buf);
              INF("Read %s to have value %lu", files[i], maxes[i]);
            }
          else
            {
              FATAL("Failed to read %s: %m", files[i]);
            }
          fclose(fp);
        }
      else
        {
          FATAL("Failed to open %s: %m", files[i]);
        }
    }

  limits.rlim_cur = limits.rlim_max =
    ((maxes[0] < maxes[1]) ? maxes[0] : maxes[1]);

  if (!setrlimit(RLIMIT_NOFILE, &limits))
    {
      return true;
    }
  else
    {
      FATAL("Failed to increase hard limit to %lu: %m", limits.rlim_cur);
    }
  return false;
}

static bool resolve_to_storage(struct sockaddr_storage *saddr,
                               const char *node, const char *service,
                               bool local)
{
  if (!node && !service)
    {
      saddr->ss_family = AF_INET6;
      // Initialized to 0 is okay.
      return true;
    }
  int err;
  struct addrinfo hints = {.ai_socktype = SOCK_STREAM,
                           .ai_flags = AI_NUMERICSERV | AI_ADDRCONFIG},
                  *res;

  if (local)
    {
      hints.ai_flags |= AI_PASSIVE;
    }
  err = getaddrinfo(node, service, &hints, &res);
  if (!err)
    {
      memcpy(saddr, res->ai_addr, res->ai_addrlen);
      freeaddrinfo(res);
      return true;
    }
  else
    {
      ERR("Failed to get addrinfo for %s:%s: %s", node, service,
          gai_strerror(err));
    }
  return false;
}

static unsigned int address_to_range(const char *str,
                                     struct sockaddr_storage **store,
                                     const char *service, bool source)
{
  unsigned int ret = 0;
  char *limiter;

  if (str && (limiter = strchr(str, '-')))
    {
      char start[INET6_ADDRSTRLEN] = { }, end[INET6_ADDRSTRLEN] = { };
      struct sockaddr_storage store_start, store_end;

      // Yeah yeah no null termination goes bad.
      strncpy(start, str, limiter - str);
      strncpy(end, limiter + 1, sizeof(end) - 1);

      INF("Using %s from %s to %s", (source) ? "sources" : "destinations",
          start, end);

      if (resolve_to_storage(&store_start, start, service, source) &&
          resolve_to_storage(&store_end, end, service, source))
        {
          uint32_t *start_addr, *end_addr;

          if (store_start.ss_family == AF_INET)
            {
              start_addr =
                &(((struct sockaddr_in *)&store_start)->sin_addr.s_addr),
              end_addr =
                &(((struct sockaddr_in *)&store_end)->sin_addr.s_addr);
            }
          else
            {
              start_addr = &(((struct sockaddr_in6 *)&store_start)
                                   ->sin6_addr.__in6_u.__u6_addr32[3]),
              end_addr = &(((struct sockaddr_in6 *)&store_end)
                                 ->sin6_addr.__in6_u.__u6_addr32[3]);
            }
          while (ntohl(*start_addr) + ret <= ntohl(*end_addr))
            {
              ret++;
            }
          INF("Found %u addresses between %s and %s", ret, start, end);

          *store = calloc(ret, sizeof(**store));
          if (*store)
            {
              unsigned int i;

              for (i = 0; i < ret; i++)
                {
                  struct sockaddr_storage *target = (*store) + i;
                  uint32_t last_val = htonl(ntohl(*start_addr) + i);

                  memcpy(target, &store_start, sizeof(**store));
                  if (store_start.ss_family == AF_INET6)
                    {
                      ((struct sockaddr_in6 *)target)
                        ->sin6_addr.__in6_u.__u6_addr32[3] = last_val;
                    }
                  else
                    {
                      ((struct sockaddr_in *)target)->sin_addr.s_addr =
                        last_val;
                    }
                  strings_set(target, false, 2);
                  DBG("Adding %s:%s as source", ipstr_src, service_str_dst);
                }
            }
          else
            {
              ERR("Failed to allocate %lu bytes for storage: %m",
                  ret * sizeof(**store));
              ret = 0;
            }
        }
    }
  else
    {
      *store = calloc(1, sizeof(**store));

      if (*store)
        {
          if (resolve_to_storage(*store, str, service, source))
            {
              ret = 1;
            }
        }
      else
        {
          ERR("Failed to allocate storage: %m");
        }
    }

  return ret;
}

static void do_exit(struct tcpspammer_ctx *ctx)
{
  uint64_t val = ctx->n_threads + 1;

  ERR("Exit called");
  if (write(ctx->eventfd, &val, sizeof(val)) != sizeof(val))
    {
      abort();
    }
}

static bool register_thread_fds(struct tcpspammer_thread_ctx *ctx)
{
  unsigned int i;
  struct epoll_event evs[] = {
    {.events = EPOLLIN, .data.fd = ctx->efd_connect},
    {.events = EPOLLIN | EPOLLONESHOT, .data.fd = ctx->main_ctx->eventfd},
    {.events = EPOLLIN, .data.fd = ctx->efd_extra},
    {.events = EPOLLIN, .data.fd = ctx->timer},
  };
  const unsigned int n_events = sizeof(evs) / sizeof(*evs);

  for (i = 0; i < n_events; i++)
    {
      if (!epoll_ctl(ctx->efd, EPOLL_CTL_ADD, evs[i].data.fd, &evs[i]))
        {
          DBG("Registered %d to main efd %d", evs[i].data.fd, ctx->efd);
        }
      else
        {
          FATAL("Failed to register %d to %d in thread %m", evs[i].data.fd,
                ctx->efd);
          break;
        }
    }
  return (i == n_events);
}

static bool so_timeout(int fd)
{
  int yes = 1;

  if (!keepidle_value)
    {
      return true;
    }

  if (!setsockopt(fd, SOL_SOCKET, SO_KEEPALIVE, &yes, sizeof(yes)))
    {
      int keepcount = 2, idle = keepidle_value, between = keepidle_value;

      if (!setsockopt(fd, IPPROTO_TCP, TCP_KEEPCNT, &keepcount, sizeof(int)) &&
          !setsockopt(fd, IPPROTO_TCP, TCP_KEEPIDLE, &idle, sizeof(int)) &&
          !setsockopt(fd, IPPROTO_TCP, TCP_KEEPINTVL, &between, sizeof(int)) &&
          !setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &yes, sizeof(int)) &&
          !setsockopt(fd, IPPROTO_TCP, TCP_SYNCNT, &keepcount, sizeof(int)))
        {
          return true;
        }
      else
        {
          FATAL("Failed to set TCP parameters on fd %d: %m", fd);
        }
    }
  else
    {
      FATAL("Failed to enable keepalive on fd %d: %m", fd);
    }
  return false;
}

enum available_sockopts
{
  SOCKOPT_INCOMING_CPU = 0x1,
  SOCKOPT_REUSEADDR = 0x2,
  SOCKOPT_REUSEPORT = 0x4,
  SOCKOPT_TIMEOUT = 0x8,
};

static bool do_sockopts(int fd, int cpu, enum available_sockopts opts)
{
  bool bret = true;
  int yes = 1;
  static _Atomic uint32_t incoming_cpu_failed = 0;

  if (!incoming_cpu_failed && opts & SOCKOPT_INCOMING_CPU &&
      setsockopt(fd, SOL_SOCKET, SO_INCOMING_CPU, &cpu, sizeof(cpu)))
    {
      FATAL("Failed to balance %d to cpu %d: %m", fd, cpu);
      incoming_cpu_failed = 1;
      // Don't fail fully, just warn once. User might just have old kernel.
    }
  if (opts & SOCKOPT_REUSEADDR &&
      setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes)))
    {
      FATAL("Failed to reuseaddr on %d: %m", fd);
      bret = false;
    }
  if (opts & SOCKOPT_REUSEPORT &&
      setsockopt(fd, SOL_SOCKET, SO_REUSEPORT, &yes, sizeof(yes)))
    {
      FATAL("Failed to set port reuse on fd %d: %m", fd);
      bret = false;
    }
  if (opts & SOCKOPT_TIMEOUT && !so_timeout(fd))
    {
      bret = false;
    }
  return bret;
}

static bool create_listeners(struct tcpspammer_thread_ctx *ctx)
{
  unsigned int i, bases_covered = 0;

  for (i = 0; i < ctx->main_ctx->n_connections; i++)
    {
      int fd;
      struct sockaddr_storage *saddr = ctx->main_ctx->sources;

      if (ctx->main_ctx->n_sources > 0)
        {
          // Cover all the given listening addresses if possible.
          saddr += (bases_covered++ * ctx->main_ctx->n_threads + ctx->cpu) %
                   ctx->main_ctx->n_sources;
        }

      fd =
        socket(saddr->ss_family, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);

      if (fd != -1)
        {
          if (do_sockopts(
                fd, ctx->cpu,
                SOCKOPT_INCOMING_CPU | SOCKOPT_REUSEADDR | SOCKOPT_REUSEPORT))
            {
              if (!bind(fd, (struct sockaddr *)saddr, sizeof(*saddr)))
                {
                  if (!listen(fd, 128))
                    {
                      struct epoll_event ev = {.data.fd = fd,
                                               .events = EPOLLIN};

                      if (!epoll_ctl(ctx->efd_extra, EPOLL_CTL_ADD, ev.data.fd,
                                     &ev))
                        {
                          strings_set(saddr, false, 1);
                          INF("Listening on %s:%s", ipstr_src, service_str_src);
                          continue;
                        }
                      else
                        {
                          FATAL("Failed to add %d to %d: %m", fd,
                                ctx->efd_extra);
                        }
                    }
                  else
                    {
                      ERR("Failed to listen on fd %d: %m", fd);
                    }
                }
              else
                {
                  strings_set(saddr, false, 0);
                  ERR("Failed to bind to %s:%s: %m", ipstr_src, service_str_src);
                }
            }
          safe_close(&fd);
        }
      else
        {
          ERR("Failed to create socket: %m");
        }
      break;
    }
  return (i == ctx->main_ctx->n_connections);
}

static bool thread_connect(struct tcpspammer_thread_ctx *ctx)
{
  struct sockaddr_storage *src = (ctx->main_ctx->sources +
                                  (rand() % ctx->main_ctx->n_sources)),
                          *dst = (ctx->main_ctx->destinations +
                                  (rand() % ctx->main_ctx->n_destinations));
  int fd = socket(src->ss_family, SOCK_STREAM | SOCK_CLOEXEC | SOCK_NONBLOCK, 0);

  if (fd != -1)
    {
      if (do_sockopts(
            fd, ctx->cpu,
            SOCKOPT_INCOMING_CPU | SOCKOPT_REUSEADDR | SOCKOPT_TIMEOUT))
        {
          if (!bind(fd, (struct sockaddr *)src, sizeof(*src)))
            {
              int ret = connect(fd, (struct sockaddr *)dst, sizeof(*dst));

              if (!ret || errno == EINPROGRESS)
                {
                  struct epoll_event ev = {.data.fd = fd, .events = EPOLLIN};
                  int efd = (!ret) ? ctx->efd_connect : ctx->efd_extra;

                  if (ret == -1)
                    {
                      ev.events |= EPOLLOUT;
                    }

                  if (!epoll_ctl(efd, EPOLL_CTL_ADD, fd, &ev))
                    {
                      ctx->active_connections++;
                      if (!ret)
                        {
                          ctx->established_connections++;
                          ctx->main_ctx->new_connections++;
                        }

                      strings_set(src, false, 2);
                      strings_set(dst, true, 2);
                      DBG("Connect%s from %s:%s to %s:%s", (ret) ? "ing" : "ed",
                          ipstr_src, service_str_dst, ipstr_dst,
                          service_str_dst);

                      return true;
                    }
                  else
                    {
                      FATAL("Failed to add just connected %d to %d: %m", fd,
                            efd);
                    }
                }
              else
                {
                  strings_set(src, false, 0);
                  ERR("Failed to connect %s:%s : %m", ipstr_src,
                      service_str_dst);
                }
            }
          else
            {
              strings_set(src, false, 0);
              ERR("Failed to bind to %s:%s: %m", ipstr_src, service_str_src);
            }
        }
      safe_close(&fd);
    }
  else
    {
      ERR("Failed to get fd: %m");
    }
  return false;
}

static int epoll_handler(int efd,
                         bool (*handler)(void *,
                                         struct epoll_event *ev),
                         void *ctx,
                         int timeout)
{
  int ret = 0;
  const unsigned int max_events = 128;
  struct epoll_event ev[max_events];

  ret = epoll_wait(efd, ev, max_events, timeout);
  if (ret >= 0)
    {
      const unsigned int n_events = ret;
      unsigned int i;

      for (i = 0; i < (unsigned int)n_events && ret != -1; i++)
        {
          if (!handler(ctx, &ev[i]))
            {
              ret = -1;
            }
          else
            {
              ret++;
            }
        }
    }
  else if (errno == EINTR)
    {
      ret = 0;
    }
  else
    {
      ERR("Failed to wait on fd %d: %m", efd);
      ret = -1;
    }
  return ret;
}

static bool thread_connections(void *ctx_ptr,
                               struct epoll_event *ev)
{
  struct tcpspammer_thread_ctx *ctx = ctx_ptr;
  char buf[BUFSIZ];
  ssize_t ret;
  int fd = ev->data.fd;

  ret = read(fd, buf, sizeof(buf));
  if (ret > 0)
    {
      DBG("Read %zu bytes from peer", ret);
      if (ctx->main_ctx->listen)
        {
          // Echo back.
          if (write(fd, buf, ret) != ret)
            {
              INF("Failed to reply with %zu data", ret);
              safe_close_decrement(&fd, ctx->efd_connect, ctx, true);
            }
        }
      else
        {
          if (ctx->main_ctx->disconnect_after_hello)
            {
              INF("Disconnecting after received reply");
              safe_close_decrement(&fd, ctx->efd_connect, ctx, true);
            }
          else if (ctx->main_ctx->explicit_echo)
            {
              struct epoll_event ev_echo = {.data.fd = fd,
                                            .events = EPOLLOUT | EPOLLONESHOT};

              if (epoll_ctl(ctx->efd_echo, EPOLL_CTL_MOD, fd, &ev_echo))
                {
                  ERR("Failed to add %d back to echo list: %m", fd);
                  safe_close_decrement(&fd, ctx->efd_connect, ctx, true);
                }
            }
        }
    }
  else
    {
      if (!(ret == -1 &&
            (errno == EWOULDBLOCK || errno == EAGAIN || errno == EINTR)))
        {
          INF("Peer %s: %m", (!ret) ? "disconnected" : "error");
          safe_close_decrement(&fd, ctx->efd_connect, ctx, true);
        }
    }
  return true;
}

static bool handle_listening(void *ctx_ptr,
                             struct epoll_event *ev)
{
  struct tcpspammer_thread_ctx *ctx = ctx_ptr;
  int fd = ev->data.fd, accepted_fd;
  struct sockaddr_storage peer;
  socklen_t peer_len = sizeof(peer);

  while ((accepted_fd = accept4(fd, (struct sockaddr *)&peer, &peer_len,
                                SOCK_NONBLOCK | SOCK_CLOEXEC)) >= 0)
    {
      struct epoll_event ev_new =
        {
          .data.fd = accepted_fd, .events = EPOLLIN
        };

      strings_set(&peer, true, 1);
      INF("Accepted connection from %s", ipstr_dst);

      if (do_sockopts(accepted_fd, ctx->cpu,
                      SOCKOPT_INCOMING_CPU | SOCKOPT_TIMEOUT))
        {
          if (!epoll_ctl(ctx->efd_connect, EPOLL_CTL_ADD, accepted_fd, &ev_new))
            {
              ctx->active_connections++;
              ctx->established_connections++;
              ctx->main_ctx->new_connections++;
              continue;
            }
          else
            {
              FATAL("Failed to add fd %d to %d: %m", accepted_fd,
                    ctx->efd_connect);
            }
        }
      safe_close(&accepted_fd);
    }
  if (!(accepted_fd == -1 &&
        (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR)))
    {
      if (errno == EMFILE || errno == ENFILE || errno == ECONNABORTED)
        {
          INF("Non-fatal error on fd %d: %m", fd);
        }
      else
        {
          ERR("Failed to accept on fd %d: %m", fd);
          return false;
        }
    }
  return true;
}

static bool send_hello(void *ctx_ptr, struct epoll_event *ev)
{
  struct tcpspammer_thread_ctx *ctx = ctx_ptr;
  ssize_t ret;
  int fd = ev->data.fd;

  ret = write(fd, ctx->main_ctx->echostring, strlen(ctx->main_ctx->echostring));
  if (ret <= 0)
    {
      ERR("Failed to write to %d: %s", fd, (!ret) ? "Disconnected" :
          strerror(errno));
      safe_close_decrement(
        &fd, (ctx->main_ctx->explicit_echo) ? ctx->efd_echo : ctx->efd_connect,
        ctx, true);
    }
  return true;
}

static bool handle_einprogress(void *ctx_ptr,
                               struct epoll_event *ev)
{
  struct tcpspammer_thread_ctx *ctx = ctx_ptr;
  int fd = ev->data.fd, err = 0;
  socklen_t err_len = sizeof(err);

  if (!getsockopt(fd, SOL_SOCKET, SO_ERROR, &err, &err_len))
    {
      struct sockaddr_storage dst;
      socklen_t dst_len = sizeof(dst);

      if (!err)
        {
          if (!getpeername(fd, (struct sockaddr *)&dst, &dst_len))
            {
              if (!connect(fd, (struct sockaddr *)&dst, dst_len))
                {
                  if (!epoll_ctl(ctx->efd_extra, EPOLL_CTL_DEL, fd, NULL))
                    {
                      struct epoll_event ev_new = {.data.fd = fd,
                                                   .events = EPOLLIN};

                      if (!epoll_ctl(ctx->efd_connect, EPOLL_CTL_ADD, fd,
                                     &ev_new))
                        {
                          struct epoll_event ev_echo = {
                            .data.fd = fd, .events = EPOLLOUT | EPOLLONESHOT};

                          if (!ctx->main_ctx->echostring ||
                              ctx->main_ctx->disconnect_after_hello ||
                              !epoll_ctl(ctx->efd_echo, EPOLL_CTL_ADD, fd,
                                         &ev_echo))
                            {
                              if (verbosity)
                                {
                                  strings_set(&dst, true, verbosity);
                                  INF("Connected to %s:%s", ipstr_dst,
                                      service_str_dst);
                                }
                              ctx->established_connections++;
                              ctx->main_ctx->new_connections++;
                              if (ctx->main_ctx->echostring &&
                                  ctx->main_ctx->disconnect_after_hello)
                                {
                                  send_hello(ctx_ptr, &ev_echo);
                                }
                              return true;
                            }
                          else
                            {
                              ERR("Failed to register fd %d to efd %d: %m", fd,
                                  ctx->efd_echo);
                            }
                        }
                      else
                        {
                          FATAL("Failed to add %d to efd %d: %m", fd,
                                ctx->efd_connect);
                        }
                    }
                  else
                    {
                      FATAL("Failed to remove %d from efd %d: %m", fd,
                            ctx->efd_extra);
                    }
                }
              else if (errno == EINPROGRESS)
                {
                  DBG("Connection on fd %d: %m", fd);
                  return true;
                }
              else
                {
                  ERR("Failed to continue connect on fd %d: %m", fd);
                }
            }
          else
            {
              ERR("Failed to get peer address on fd %d: %m", fd);
            }
        }
      else
        {
          if (verbosity > 0)
            {
              ERR("Failed to connect: %s", strerror(err));
              if (!getpeername(fd, (struct sockaddr *)&dst, &dst_len))
                {
                  strings_set(&dst, true, 0);
                  ERR("Endpoint was %s:%s", ipstr_dst, service_str_dst);
                }
              else
                {
                  ERR("Couldn't get it: %m");
                }
            }
        }
    }
  else
    {
      ERR("Failed to get SO_ERROR for %d: %m", fd);
    }
  safe_close_decrement(&fd, ctx->efd_extra, ctx, false);
  return true;
}

static bool should_exit(int eventfd)
{
  uint64_t data;
  ssize_t ret = read(eventfd, &data, sizeof(data));

  if (ret == sizeof(data))
    {
      INF("Registered exit call from eventfd");
      return true;
    }
  else
    {
      ERR("Failed to read eventfd %d: %s", eventfd,
          (!ret) ? "closed" : strerror(errno));
    }
  return false;
}

static bool handle_thread_main_efd(void *ctx_ptr,
                                   struct epoll_event *ev)
{
  struct tcpspammer_thread_ctx *ctx = ctx_ptr;
  bool continue_loop = false;
  int fd = ev->data.fd;

  DBG("Thread has data in fd %d", ev->data.fd);
  if (fd == ctx->main_ctx->eventfd)
    {
      continue_loop = !should_exit(ctx->main_ctx->eventfd);
    }
  else if (fd == ctx->efd_connect)
    {
      continue_loop =
        (epoll_handler(ctx->efd_connect, thread_connections, ctx, 0) >= 0);
    }
  else if (fd == ctx->efd_extra)
    {
      INF("Data in %s", (ctx->main_ctx->listen) ? "listening" : "einprogress");
      continue_loop =
        (epoll_handler(
           ctx->efd_extra,
           (ctx->main_ctx->listen) ? handle_listening : handle_einprogress, ctx,
           0) >= 0);
      ;
    }
  else if (fd == ctx->timer)
    {
      continue_loop = (read_timer(ctx->timer) >= 0);
      if (ctx->main_ctx->echostring && ctx->efd_echo != -1)
        {
          while (epoll_handler(ctx->efd_echo, send_hello, ctx, 0) > 0)
            {
              // Send hellos.
            }
        }
    }
  else
    {
      abort();
    }

  return continue_loop;
}

static void tcpspammer_thread_loop(struct tcpspammer_thread_ctx *ctx)
{
  int n_events;
  bool back_off = false;

  DBG("Starting thread loop");
  do
    {
      // If we don't have all connections up yet, do more on timeout.
      int timeout = (ctx->main_ctx->listen ||
                     ctx->active_connections == ctx->main_ctx->n_connections ||
                     back_off)
                      ? -1
                      : 0;

      back_off = false;

      n_events = epoll_handler(ctx->efd, handle_thread_main_efd, ctx, timeout);
      if (!n_events && !ctx->main_ctx->listen)
        {
          const unsigned int connections_missing =
            ctx->main_ctx->n_connections - ctx->active_connections;
          unsigned int i;

          DBG("Timeout. Asking for %u more connections", connections_missing);

          for (i = 0; i < connections_missing && i < max_conns_per_loop &&
                      (!ctx->main_ctx->n_connections_per_sec ||
                       ctx->main_ctx->new_connections <
                         ctx->main_ctx->n_connections_per_sec);
               i++)
            {
              if (!thread_connect(ctx))
                {
                  back_off = true;
                  break;
                }
            }
        }
    }
  while (n_events >= 0);
}

static void *tcpspammer_thread_run(void *arg)
{
  struct tcpspammer_thread_ctx *ctx = arg;
  bool success = false;

  DBG("Starting thread");
  ctx->cpu = sched_getcpu();
  ctx->timer = -1;
  ctx->efd_echo = -1;
  ctx->efd_extra = epoll_create1(EPOLL_CLOEXEC);
  ctx->efd = epoll_create1(EPOLL_CLOEXEC);
  ctx->efd_connect = epoll_create1(EPOLL_CLOEXEC);
  if (ctx->efd != -1 || ctx->efd_extra != -1 || ctx->efd_connect != -1)
    {
      if (ctx->main_ctx->listen || !ctx->main_ctx->explicit_echo ||
          ((ctx->efd_echo = epoll_create1(EPOLL_CLOEXEC)) != -1))
        {
          if ((ctx->timer = init_timer(ctx->main_ctx->echo_timer)) != -1)
            {
              if (register_thread_fds(ctx))
                {
                  if (!ctx->main_ctx->listen || create_listeners(ctx))
                    {
                      success = true;
                      tcpspammer_thread_loop(ctx);
                    }
                }
            }
        }
      else
        {
          ERR("Failed to create echo efd");
        }
    }
  else
    {
      ERR("Failed to create efd: %m");
    }
  INF("Thread exiting");
  safe_close(&ctx->efd);
  safe_close(&ctx->efd_extra);
  safe_close(&ctx->efd_echo);
  safe_close(&ctx->efd_connect);
  safe_close(&ctx->timer);
  if (!success)
    {
      do_exit(ctx->main_ctx);
    }

  ctx->main_ctx->n_finished++;
  pthread_exit(NULL);
}

static bool init_threads(struct tcpspammer_ctx *ctx, const char *sources,
                         const char *destinations, const char *service,
                         const char *port)
{
  unsigned int i;

  ctx->n_sources = address_to_range(sources, &ctx->sources, service, true);
  ctx->n_destinations = address_to_range(destinations, &ctx->destinations,
                                         port, false);
  if (ctx->n_sources && ctx->n_destinations)
    {
      if (!sources && destinations)
        {
          ctx->sources[0].ss_family = ctx->destinations[0].ss_family;
        }

      for (i = 0; i < ctx->n_threads; i++)
        {
          ctx->threads[i].main_ctx = ctx;
          cpu_set_t cpus;

          CPU_ZERO(&cpus);
          CPU_SET(i, &cpus);
          if (!pthread_attr_init(&ctx->threads[i].attr) &&
              !pthread_attr_setaffinity_np(&ctx->threads[i].attr, sizeof(cpus),
                                          &cpus))
            {
              if (!pthread_create(&ctx->threads[i].thread, &ctx->threads[i].attr,
                                 tcpspammer_thread_run, &ctx->threads[i]))
                {
                  INF("Initialized thread %u", i);
                }
              else
                {
                  FATAL("Failed to start thread %u: %m", i);
                }
            }
          else
            {
              FATAL("Failed to set affinity for thread %d: %m", i);
            }
        }
      return true;
    }
  return false;
}

static bool init_efds(struct tcpspammer_ctx *ctx)
{
  ctx->efd = epoll_create1(EPOLL_CLOEXEC);

  if (ctx->efd != -1)
    {
      unsigned int i;
      struct epoll_event evs[] =
        {
            {
              .events = EPOLLIN,
              .data.fd = ctx->signalfd
            },
            {
              .events = EPOLLIN,
              .data.fd = ctx->timerfd
            },
            {
              .events = EPOLLIN | EPOLLONESHOT,
              .data.fd = ctx->eventfd
            },
            {
              .events = EPOLLIN,
              .data.fd = logpipe[0]
            },
        };

      for (i = 0; i < sizeof(evs)/sizeof(*evs); i++)
        {
          if (epoll_ctl(ctx->efd, EPOLL_CTL_ADD, evs[i].data.fd, &evs[i]))
            {
              FATAL("Failed to %u add %d to %d: %m", i, evs[i].data.fd,
                    ctx->efd);
              break;
            }
        }
      if (i == sizeof(evs)/sizeof(*evs))
        {
          return true;
        }
    }
  else
    {
      FATAL("Failed to create efd: %m");
    }
  return false;
}

static bool handle_signalfd(struct tcpspammer_ctx *ctx)
{
  struct signalfd_siginfo sinfo;

  while ((read(ctx->signalfd, &sinfo, sizeof(sinfo))) > 0)
    {
      if (sinfo.ssi_signo == SIGTERM || sinfo.ssi_signo == SIGINT)
        {
          printf("Exit signal received in main thread\n");
          do_exit(ctx);
        }
      else if (sinfo.ssi_signo == SIGPIPE)
        {
          INF("Received sigpipe");
        }
      else
        {
          ERR("Received unknown signal %s", strsignal(sinfo.ssi_signo));
        }
    }
  return true; // Will exit from eventfd.
}

static bool handle_timer(struct tcpspammer_ctx *ctx)
{
  int val = read_timer(ctx->timerfd);

  if (val > 0)
    {
      uint64_t active = 0, established = 0, new_conns = 0;
      unsigned int i;

      DBG("Timer expired %d times", val);

      for (i = 0; i < ctx->n_threads; i++)
        {
          active += ctx->threads[i].active_connections;
          established += ctx->threads[i].established_connections;
        }
      new_conns += ctx->new_connections;
      printf("Active: %lu, Established: %lu, New: %lu\n", active, established,
             new_conns);
      ctx->new_connections = 0;
    }
  else
    {
      ERR("Shouldn't happen from timerfd: %m");
      return false;
    }
  return true;
}

static bool handle_logpipe(__attribute__((unused)) struct tcpspammer_ctx *ctx)
{
  char buf[BUFSIZ];
  ssize_t len;
  unsigned int max_iter = 512;

  while ((len = read(logpipe[0], buf, sizeof(buf))) > 0 && --max_iter)
    {
      if (write(STDOUT_FILENO, buf, len) == -1 &&
          (errno == EBADF || errno == EINVAL || errno == EPIPE))
        {
          return false;
        }
    }
  if (!max_iter)
    {
      printf("Almost busylooped pipe read\n");
    }
  else if (!(len == -1 &&
             (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR)))
    {
      fprintf(stderr, "Failed to read from logpipe: %m\n");
      return false;
    }
  return true;
}

static bool main_loop_handler(void *ctx_ptr, struct epoll_event *ev)
{
  struct tcpspammer_ctx *ctx = ctx_ptr;
  int fd = ev->data.fd;
  bool bret = true;

  if (fd == ctx->eventfd)
    {
      bret = !should_exit(ctx->eventfd);
      INF("Main thread exit");
    }
  else if (fd == ctx->signalfd)
    {
      bret = handle_signalfd(ctx);
    }
  else if (fd == ctx->timerfd)
    {
      bret = handle_timer(ctx);
    }
  else if (fd == logpipe[0])
    {
      bret = handle_logpipe(ctx);
    }
  return bret;
}

static void usage(const char *bin)
{
  fprintf(stdout,
          "(%s) Creates arbitrary number of TCP connections to peer\n"
          "     spawning per core threads each handling connections.\n"
          "Usage: %s [-lnspd]\n\n"
          "Options:\n"
          "          -l         Listens for connections\n"
          "          -L <port>  Local port. Defaults to 7 when listening.\n"
          "          -n <count> Specify connections per thread.\n"
          "          -m         Maximize fd soft limit.\n"
          "          -M         Maximize fds presuming CAP_SYS_RESOURCE .\n"
          "          -s <src>   Source for connections or listen IP\n"
          "          -k <sec>   TCP keepidle and keepintvl value. Default 5\n"
          "                     A value of 0 disables keepalive\n"
          "          -d <dst>   Destination for connections if not listening\n"
          "          -N <count> Establish a maximum of count connections per second\n"
          "          -i <count> Maximum number of connect-calls per loop.\n"
          "                     Defaults to 8192.\n"
          "          -p <port>  Destination port. Defaults to 7\n"
          "          -w         Reduce verbosity\n"
          "          -v         Increase verbosity\n\n"
          "          -a <str>   Echo string sent every second (or -e value)\n"
          "          -e <ms>    Interval for echo timer\n"
          "          -E         Disconnect after receiving reply for hello\n"
          "The source and destinations can be a range. e.g.\n"
          "-s 10.0.0.1-10.0.0.100 which works also on IPv6. Note though that\n"
          "-s fd10::10-fd10::30 is not 20 addresses.",
          bin, bin);
}

int main(int argc, char **argv)
{
  int c, exit_val = EXIT_SUCCESS;
  struct tcpspammer_ctx ctx =
    {
      .n_threads = get_nprocs(),
      .eventfd = eventfd(0, EFD_CLOEXEC | EFD_NONBLOCK | EFD_SEMAPHORE),
      .signalfd = signalfd_init(),
      .timerfd = -1,
      .echo_timer = {1, 0},
      .n_connections = 1
    };
  struct tcpspammer_thread_ctx thread_ctxs[ctx.n_threads];
  const char *sources = NULL, *destinations = NULL, *service = NULL,
        *port = NULL;
  const char *default_service = "7";

  ctx.threads = thread_ctxs;
  memset(thread_ctxs, 0, sizeof(thread_ctxs));
  srand(time(NULL));
  while ((c = getopt(argc, argv, "ls:d:n:p:hvmMe:a:wL:Ek:i:N:")) != -1)
    {
      switch(c)
        {
        case 'l':
          ctx.listen = true;
          break;
        case 'n':
          ctx.n_connections = atol(optarg);
          break;
        case 's':
          sources = optarg;
          break;
        case 'L':
          service = optarg;
          break;
        case 'p':
          port = optarg;
          break;
        case 'M':
          if (!max_nofile())
            {
              exit_val = EXIT_FAILURE;
            }
          break;
        case 'm':
          if (!max_nofile_soft())
            {
              exit_val = EXIT_FAILURE;
            }
          break;
        case 'd':
          destinations = optarg;
          break;
        case 'a':
          ctx.echostring = optarg;
          break;
        case 'N':
          ctx.n_connections_per_sec = atoi(optarg);
          break;
        case 'e':
            {
              int val = atoi(optarg);

              ctx.echo_timer.tv_sec = (val / 1000);
              ctx.echo_timer.tv_nsec = (val % 1000);
            }
          ctx.explicit_echo = true;
          break;
        case 'E':
          ctx.disconnect_after_hello = true;
          break;
        case 'v':
          verbosity++;
          break;
        case 'w':
          verbosity--;
          break;
        case 'k':
          keepidle_value = atoi(optarg);
          break;
        case 'i':
          max_conns_per_loop = atoi(optarg);
          break;
        default:
          FATAL("Unknown arg %c", c);
          /* fall through */
        case 'h':
          exit_val = EXIT_FAILURE;
          usage(argv[0]);
          break;
        }
    }
  if (ctx.listen && !service)
    {
      service = default_service;
    }
  else if (!ctx.listen && !port)
    {
      port = default_service;
    }

  if (exit_val != EXIT_SUCCESS)
    {
    }
  else if (!ctx.listen && !destinations)
    {
      FATAL("No destinations set for connections");
      usage(argv[0]);
    }
  else if (ctx.eventfd == -1)
    {
      FATAL("Failed to create eventfd: %m");
    }
  else if ((ctx.timerfd = init_timer((struct timespec){1,0})) == -1)
    {
      FATAL("Failed to init timer");
    }
  else if (!init_efds(&ctx))
    {
      FATAL("Failed to init main efds");
    }
  else if (!init_threads(&ctx, sources, destinations, service, port))
    {
      FATAL("Failed to initialize threads");
    }
  else
    {
      unsigned int i;

      while (epoll_handler(ctx.efd, main_loop_handler, &ctx, -1) >= 0)
        {
          // Loop it.
        }

      // Read logpipe in a busyloop until threads have exited
      while (ctx.n_finished < ctx.n_threads)
        {
          handle_logpipe(&ctx);
        }
      INF("All threads joined");
      for (i = 0; i < ctx.n_threads; i++)
        {
          if (pthread_join(ctx.threads[i].thread, NULL))
            {
              INF("Failed to join thread %d: %m", i);
            }
          pthread_attr_destroy(&ctx.threads[i].attr);
        }
    }
  // Drain pipe at the end.
  handle_logpipe(&ctx);
  free(ctx.sources);
  free(ctx.destinations);
  safe_close(&ctx.eventfd);
  safe_close(&ctx.efd);
  safe_close(&ctx.signalfd);
  safe_close(&ctx.timerfd);
  safe_close(&logpipe[0]);
  safe_close(&logpipe[1]);

  printf("Exiting\n");

  return exit_val;
}
