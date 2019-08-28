# TCP spammer.

Test tool for stressing components with the number of TCP connections.

## Example:

Add some bogus IPv6 addresses to loopback for testing:

```
# for i in $(seq 10 40) ; do ip -6 a a fd99::$(printf "%x" $i)/64 dev lo ; done
# for i in $(seq 100 140) ; do ip -6 a a fd99::$(printf "%x" $i)/64 dev lo ; done
```

Now start tcpspammer listening on those addresses:

```
# ./tcpspammer -l -s fd99::a-fd99::28 -L 5000 -w -M -n 3
Active: 0, Established: 0
Active: 0, Established: 0
Active: 0, Established: 0
```

It will report once a second how many connections are active. I happen to have
12 cores so filling up all 30 ipv6 addresses can be done by setting the -n for
connection count to 3, meaning 3\*12 listeners are active.

Lets start the client-side:

```
./tcpspammer  -s fd99::64-fd99::8c -d fd99::a-fd99::28 -p 5000 -M -w -n 40000
Active: 136478, Established: 0
Active: 206988, Established: 0
```

We can see the server side ramping up with connections:

```
Active: 0, Established: 0
Active: 97951, Established: 97951
Active: 199261, Established: 199261
Active: 215469, Established: 215469
Active: 219231, Established: 219231
```

Active and established on the client side signify on how many `connect` has been
called and how many have given a successful POLLOUT results, as in handshake has
finished.  Active and established on the listening side are the same thing as an
accepted fd is immediately usable.

tcpspammer is quite aggressive in getting new connections if it hasn't yet
connected all of its connections. This is very visible if it cannot get enough
file descriptors.

## Compilation

Needs just pthread and \_GNU\_SOURCE e.g.

```
clang -D_GNU_SOURCE -Wall -Wextra -O3 -lpthread -std=c11 ./tcpspammer.c -o tcpspammer
```

## Test setups.

To get the most out of this tool it's recommended to dramatically increase the
OS fd limits. This means two files sysctl variables. Here they're increased to
~40 million:

```
sysctl -w fs.file-max=39253532
sysctl -w fs.nr_open=39253532
sysctl -p
```

When starting tcpspammer, use the -M switch (not -m) to increase the open file
count to the minimum of the two sysctl parameters.

## Internals

tcpspammer employes epoll, timer, signal, pipe and event file descriptors. Heap
allocation is minimal and only for source and destination addresses. All
connections are added to epoll file descriptors and their resource use is only
through the file descriptor.

The connections are originated from threads bound per CPU core, for which the
sockets are also CPU bound.

Logging is done through a single pipe, allowing atomic writes to it, which the
main thread writes to stdout.
