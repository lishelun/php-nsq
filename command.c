/*
  +----------------------------------------------------------------------+
  | Copyright (c) 1997-2017 The PHP Group                                |
  +----------------------------------------------------------------------+
  | This source file is subject to version 3.01 of the PHP license,      |
  | that is bundled with this package in the file LICENSE, and is        |
  | available through the world-wide-web at the following url:           |
  | http://www.php.net/license/3_01.txt                                  |
  | If you did not receive a copy of the PHP license and are unable to   |
  | obtain it through the world-wide-web, please send a note to          |
  | license@php.net so we can mail you a copy immediately.               |
  +----------------------------------------------------------------------+
  | Author: Zhenyu Wu      <wuzhenyu@kuangjue.com>                       |
  +----------------------------------------------------------------------+
*/

#include <stdio.h>
#include <unistd.h>
#include <event2/event.h>
#include <event2/buffer.h>
#include <event2/bufferevent.h>
#include <event2/bufferevent_struct.h>

const static char *NEW_LINE = "\n";
const static int MAX_BUF_SIZE = 128;

void nsq_subscribe(struct bufferevent *bev, const char *topic, const char *channel) {
    char b[MAX_BUF_SIZE];
    size_t n;
    n = sprintf(b, "SUB %s %s%s", topic, channel, NEW_LINE);
    bufferevent_write(bev, b, n);
}

void nsq_ready(struct bufferevent *bev, int count) {
    char b[MAX_BUF_SIZE];
    size_t n;
    n = sprintf(b, "RDY %d%s", count, NEW_LINE);
     // 调试输出
    bufferevent_write(bev, b, n);
    int res = bufferevent_flush(bev, EV_WRITE, BEV_FLUSH | BEV_FINISHED);
    // if (res != 0) {
        // fprintf(stderr, "NSQ ERROR: Failed to flush buffer (res=%d)\n", res);
    // }
    // fprintf(stderr, "NSQ DEBUG: Sending RDY command: %.*s", (int)n, b);
    // size_t pending = evbuffer_get_length(bufferevent_get_output(bev));
    // fprintf(stderr, "NSQ DEBUG: Output buffer pending: %zu bytes\n", pending);


}

void nsq_finish(struct bufferevent *bev, const char *id) {
    char b[MAX_BUF_SIZE];

    size_t pos = 0;

    memcpy(b + pos, "FIN ", 4);
    pos += 4;

    memcpy(b + pos, id, 16);
    pos += 16;

    memcpy(b + pos, NEW_LINE, strlen(NEW_LINE));
    pos += strlen(NEW_LINE);

    bufferevent_write(bev, b, pos);
}

void nsq_touch(struct bufferevent *bev, const char *id) {
    char b[MAX_BUF_SIZE];
    // n = sprintf(b, "TOUCH %s%s", id, NEW_LINE);
    evutil_socket_t fd = bufferevent_getfd(bev);
    size_t pos = 0;
    memcpy(b + pos, "TOUCH ", 6);
    pos += 6;
    memcpy(b + pos, id, 16);
    pos += 16;
    memcpy(b + pos, NEW_LINE, strlen(NEW_LINE));
    pos += strlen(NEW_LINE);
    int res = write(fd, b, pos);
}


void nsq_nop(struct bufferevent *bev) {
    char b[MAX_BUF_SIZE];
    size_t n;
    n = sprintf(b, "NOP%s", NEW_LINE);
    bufferevent_write(bev, b, n);
}

void nsq_requeue(struct bufferevent *bev, const char *id, int timeout_ms) {
    // char b[MAX_BUF_SIZE];
    // size_t n;
    // n = sprintf(b, "REQ %s %d%s", id, timeout_ms, NEW_LINE);
    // bufferevent_write(bev, b, n);

    char b[MAX_BUF_SIZE];
    size_t pos = 0;

    memcpy(b + pos, "REQ ", 4);
    pos += 4;

    memcpy(b + pos, id, 16);
    pos += 16;

    b[pos++] = ' ';

    pos += sprintf(b + pos, "%d", timeout_ms);

    memcpy(b + pos, NEW_LINE, strlen(NEW_LINE));
    pos += strlen(NEW_LINE);

    bufferevent_write(bev, b, pos);
}
