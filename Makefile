CFLAGS=-O3 -D_GNU_SOURCE -std=c11 -Wall -Wextra -Wshadow

all: tcpspammer

tcpspammer: tcpspammer.o
	$(CC) ${CFLAGS} -pthread -o $@ $^

clean:
	rm tcpspammer tcpspammer.o
