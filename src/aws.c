// SPDX-License-Identifier: BSD-3-Clause

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <sys/types.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <sys/sendfile.h>
#include <sys/eventfd.h>
#include <libaio.h>
#include <errno.h>

#include "aws.h"
#include "utils/util.h"
#include "utils/debug.h"
#include "utils/sock_util.h"
#include "utils/w_epoll.h"
#define MAX_EVENTS 1024
#define MAX_CONNECTIONS 1024

/* server socket file descriptor */
static int listenfd;

/* epoll file descriptor */
static int epollfd;

static io_context_t ctx;

static int aws_on_path_cb(http_parser *p, const char *buf, size_t len)
{
	struct connection *conn = (struct connection *)p->data;

	memcpy(conn->request_path, buf, len);
	conn->request_path[len] = '\0';
	conn->have_path = 1;

	return 0;
}

static void connection_prepare_send_reply_header(struct connection *conn)
{
	/* TODO: Prepare the connection buffer to send the reply header. */
	memset(conn->send_buffer, 0, sizeof(conn->send_buffer));
	sprintf(conn->send_buffer,
			"HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\n\r\n");
	conn->send_len = strlen(conn->send_buffer);
}

static void connection_prepare_send_404(struct connection *conn)
{
	/* TODO: Prepare the connection buffer to send the 404 header. */
	memset(conn->send_buffer, 0, sizeof(conn->send_buffer));
    sprintf(conn->send_buffer,
            "HTTP/1.1 404 Not Found\r\nContent-Type: text/plain\r\n\r\n");
    conn->send_len = strlen(conn->send_buffer);

}

static enum resource_type connection_get_resource_type(struct connection *conn)
{
	/* TODO: Get resource type depending on request path/filename. Filename should
	 * point to the static or dynamic folder.
	 */
	if (conn->have_path) {
		if (strncmp(conn->request_path, AWS_REL_STATIC_FOLDER, strlen(AWS_REL_STATIC_FOLDER)) == 0) {
			return RESOURCE_TYPE_STATIC;
		}
		else if (strncmp(conn->request_path, AWS_REL_DYNAMIC_FOLDER, strlen(AWS_REL_DYNAMIC_FOLDER)) == 0) {
			return RESOURCE_TYPE_DYNAMIC;
		}
	}
	return RESOURCE_TYPE_NONE;
}


struct connection *connection_create(int sockfd)
{
	/* TODO: Initialize connection structure on given socket. */

	struct connection *conn = calloc(1, sizeof(struct connection));

	conn->sockfd = sockfd;
	conn->eventfd = eventfd(0, EFD_NONBLOCK);

	return conn;
}

void connection_start_async_io(struct connection *conn)
{
	/* TODO: Start asynchronous operation (read from file).
	 * Use io_submit(2) & friends for reading data asynchronously.
	 */
	if (conn->fd < 0) {
		conn->fd = open(conn->filename, O_RDONLY | O_NONBLOCK);
		if (conn->fd < 0) {
			ERR("open failed\n");
			exit(1);
		}
	}
	memset(&conn->iocb, 0, sizeof(struct iocb));

	// prepare the iocb for async read
	io_prep_pread(&(conn->iocb), conn->fd, conn->recv_buffer, sizeof(conn->recv_buffer), conn->file_pos);

	// set the eventfd for notification
	io_set_eventfd(&(conn->iocb), conn->eventfd);

	conn->piocb[0] = &(conn->iocb);

	int res = io_submit(conn->ctx, 1, conn->piocb);
	if (res < 0) {
		ERR("io_submit failed\n");
		close(conn->fd);
		conn->fd = -1;
		exit(1);
	}
	conn->state = STATE_ASYNC_ONGOING;
}

void connection_remove(struct connection *conn)
{
	w_epoll_remove_fd(epollfd, conn->sockfd);
	close(conn->sockfd);
	free(conn);
}

void handle_new_connection(void)
{
	/* TODO: Handle a new connection request on the server socket. */

	/* TODO: Accept new connection. */

	/* TODO: Set socket to be non-blocking. */

	/* TODO: Instantiate new connection handler. */

	/* TODO: Add socket to epoll. */

	/* TODO: Initialize HTTP_REQUEST parser. */

	struct sockaddr_in client_addr;
	socklen_t client_len = sizeof(client_addr);

	int newfd = accept(listenfd, (struct sockaddr *)&client_addr, &client_len);

	fcntl(newfd, F_SETFL | fcntl(newfd, F_GETFL, 0) | O_NONBLOCK);

	struct connection *conn = connection_create(newfd);

	w_epoll_add_fd_in(epollfd, newfd);

	http_parser_init(&(conn->request_parser), HTTP_REQUEST);
	conn->request_parser.data = conn;

}

void receive_data(struct connection *conn)
{
	/* TODO: Receive message on socket.
	 * Store message in recv_buffer in struct connection.
	 */
	if (conn == NULL) {
		return;
	}
	memset(conn->recv_buffer, 0, sizeof(conn->recv_buffer));
	conn->recv_len = recv(conn->sockfd, conn->recv_buffer, sizeof(conn->recv_buffer), 0);
	if (conn->recv_len < 0) {
		if (errno == EAGAIN || errno == EWOULDBLOCK) {
			return;
		} else {
			ERR("recv failed\n");
			connection_remove(conn);
			return;
		}
	}
	if (conn->recv_len == 0) {
		connection_remove(conn);
		return;
	}
}

int connection_open_file(struct connection *conn)
{
	/* TODO: Open file and update connection fields. */
	if (conn == NULL) {
		return -1;
	}
	conn->fd = open(conn->filename, O_RDONLY | O_NONBLOCK);
	if (conn->fd < 0) {
		ERR("open failed\n");
		return -1;
	}
	struct stat file_stat;
	if (fstat(conn->fd, &file_stat) < 0) {
		ERR("fstat failed\n");
		close(conn->fd);
		conn->fd = -1;
		return -1;
	}
	conn->file_size = file_stat.st_size;
	return 0;
}

void connection_complete_async_io(struct connection *conn)
{
	/* TODO: Complete asynchronous operation; operation returns successfully.
	 * Prepare socket for sending.
	 */
	if (conn == NULL) {
		return;
	}
	struct io_event events[1];
	struct timespec timeout = {0, 0};
	int num_events = io_getevents(conn->ctx, 1, 1, events, &timeout);
	if (num_events < 0) {
		ERR("io_getevents failed\n");
		close(conn->fd);
		conn->fd = -1;
		connection_remove(conn);
		return;
	}
	if (num_events == 0) {
		return;
	}
	if (events[0].res < 0) {
		ERR("io_getevents failed\n");
		close(conn->fd);
		conn->fd = -1;
		connection_remove(conn);
		return;
	}
	conn->async_read_len = events[0].res;
	memcpy(conn->send_buffer, conn->recv_buffer, conn->async_read_len);
	conn->send_len = conn->async_read_len;
	conn->send_pos = 0;
	conn->state = STATE_SENDING_DATA;
}

int parse_header(struct connection *conn)
{
	/* TODO: Parse the HTTP header and extract the file path. */
	/* Use mostly null settings except for on_path callback. */
	http_parser_settings settings_on_path = {
		.on_message_begin = 0,
		.on_header_field = 0,
		.on_header_value = 0,
		.on_path = aws_on_path_cb,
		.on_url = 0,
		.on_fragment = 0,
		.on_query_string = 0,
		.on_body = 0,
		.on_headers_complete = 0,
		.on_message_complete = 0
	};
	http_parser_execute(&conn->request_parser, &settings_on_path, conn->recv_buffer, conn->recv_len);
	return 0;
}

enum connection_state connection_send_static(struct connection *conn)
{
	/* TODO: Send static data using sendfile(2). */
	if (conn == NULL) {
		return STATE_CONNECTION_CLOSED;
	}
	off_t offset = conn->file_pos;
	ssize_t sent = sendfile(conn->sockfd, conn->fd, &offset, conn->file_size - conn->file_pos);

	if (sent < 0) {
		if (errno == EAGAIN || errno == EWOULDBLOCK) {
			return STATE_SENDING_DATA;
		} else {
			ERR("sendfile failed\n");
			close(conn->fd);
			conn->fd = -1;
			connection_remove(conn);
			return STATE_CONNECTION_CLOSED;
		}
	}

	conn->file_pos += sent;
	if (conn->file_pos >= conn->file_size)
		return STATE_DATA_SENT;
	return STATE_SENDING_DATA;
}

int connection_send_data(struct connection *conn)
{
	/* May be used as a helper function. */
	/* TODO: Send as much data as possible from the connection send buffer.
	 * Returns the number of bytes sent or -1 if an error occurred
	 */
	if (conn == NULL) {
		return -1;
	}
	ssize_t total_sent = 0;
	while (conn->send_pos < conn->send_len) {
		ssize_t sent = send(conn->sockfd, conn->send_buffer + conn->send_pos, conn->send_len - conn->send_pos, 0);
		if (sent < 0) {
			if (errno == EAGAIN || errno == EWOULDBLOCK) {
				break;
			} else {
				ERR("send failed\n");
				connection_remove(conn);
				return -1;
			}
		}
		total_sent += sent;
		conn->send_pos += sent;
	}
	return total_sent;
}


int connection_send_dynamic(struct connection *conn)
{
	/* TODO: Read data asynchronously.
	 * Returns 0 on success and -1 on error.
	 */
	if (conn == NULL) {
		return -1;
	}
	if (conn->async_read_len == 0) {
		connection_start_async_io(conn);
		return 0;
	}
	if (conn->async_read_len < sizeof(conn->recv_buffer)) {
		conn->state = STATE_SENDING_DATA;
		return 0;
	}
	connection_complete_async_io(conn);
	return 0;
}


void handle_input(struct connection *conn)
{
	/* TODO: Handle input information: may be a new message or notification of
	 * completion of an asynchronous I/O operation.
	 */

	switch (conn->state) {
	case STATE_INITIAL:
		receive_data(conn);
		parse_header(conn);
		conn->res_type = connection_get_resource_type(conn);
		if (conn->res_type == RESOURCE_TYPE_NONE) {
			connection_prepare_send_404(conn);
			conn->state = STATE_SENDING_404;
		} else if (conn->res_type == RESOURCE_TYPE_STATIC) {
			connection_prepare_send_reply_header(conn);
			conn->state = STATE_SENDING_HEADER;
		} else if (conn->res_type == RESOURCE_TYPE_DYNAMIC) {
			connection_prepare_send_reply_header(conn);
			conn->state = STATE_SENDING_HEADER;
		}
		break;
	case STATE_ASYNC_ONGOING:
		connection_complete_async_io(conn);
		break;
	case STATE_SENDING_HEADER:
		connection_send_data(conn);
		if (conn->send_pos >= conn->send_len) {
			if (conn->res_type == RESOURCE_TYPE_STATIC) {
				conn->state = STATE_SENDING_DATA;
			} else if (conn->res_type == RESOURCE_TYPE_DYNAMIC) {
				conn->state = STATE_SENDING_DATA;
			}
		}
		break;
	case STATE_SENDING_DATA:
		if (conn->res_type == RESOURCE_TYPE_STATIC) {
			connection_send_static(conn);
		} else if (conn->res_type == RESOURCE_TYPE_DYNAMIC) {
			connection_send_dynamic(conn);
		}
		break;
	case STATE_SENDING_404:
		connection_send_data(conn);
		if (conn->send_pos >= conn->send_len) {
			conn->state = STATE_CONNECTION_CLOSED;
		}
		break;
	case STATE_DATA_SENT:
		connection_remove(conn);
		break;
	case STATE_404_SENT:
		connection_remove(conn);
		break;
	case STATE_CONNECTION_CLOSED:
		connection_remove(conn);
		break;
	default:
		break;
	}
}

void handle_output(struct connection *conn)
{
	/* TODO: Handle output information: may be a new valid requests or notification of
	 * completion of an asynchronous I/O operation or invalid requests.
	 */

	switch (conn->state) {
	case STATE_INITIAL:
		break;
	case STATE_ASYNC_ONGOING:
		break;
	case STATE_SENDING_HEADER:
		connection_send_data(conn);
		if (conn->send_pos >= conn->send_len) {
			if (conn->res_type == RESOURCE_TYPE_STATIC) {
				conn->state = STATE_SENDING_DATA;
			} else if (conn->res_type == RESOURCE_TYPE_DYNAMIC) {
				conn->state = STATE_SENDING_DATA;
			}
		}
		break;
	case STATE_SENDING_DATA:
		if (conn->res_type == RESOURCE_TYPE_STATIC) {
			conn->state = connection_send_static(conn);
		} else if (conn->res_type == RESOURCE_TYPE_DYNAMIC) {
			conn->state = connection_send_dynamic(conn);
		}
		break;
	case STATE_SENDING_404:
		connection_send_data(conn);
		if (conn->send_pos >= conn->send_len) {
			conn->state = STATE_CONNECTION_CLOSED;
		}
		break;
	case STATE_DATA_SENT:
		connection_remove(conn);
		break;
	case STATE_404_SENT:
		connection_remove(conn);
		break;
	case STATE_CONNECTION_CLOSED:
		connection_remove(conn);
		break;
	default:
		break;
	}
}

void handle_client(uint32_t event, struct connection *conn)
{
	/* TODO: Handle new client. There can be input and output connections.
	 * Take care of what happened at the end of a connection.
	 */
	switch (event) {
	case EPOLLIN:
		handle_input(conn);
		break;
	case EPOLLOUT:
		handle_output(conn);
		break;
	default:
		break;
	}	
}

int main(void)
{
	epollfd = epoll_create(1);
	DIE(epollfd < 0, "epoll_create");

	listenfd = socket(AF_INET, SOCK_STREAM, 0);
	DIE(listenfd < 0, "socket");

	struct sockaddr_in serv_addr;

	memset(&serv_addr, 0, sizeof(serv_addr));
	serv_addr.sin_family = AF_INET;
	serv_addr.sin_port = htons(AWS_LISTEN_PORT);
	serv_addr.sin_addr.s_addr = INADDR_ANY;

	bind(listenfd, (struct sockaddr *)&serv_addr, sizeof(serv_addr));

	listen(listenfd, MAX_CONNECTIONS);

	epollfd = epoll_create(1);
	DIE(epollfd < 0, "epoll_create");

	w_epoll_add_fd_in(epollfd, listenfd);

	/* Uncomment the following line for debugging. */
	dlog(LOG_INFO, "Server waiting for connections on port %d\n", AWS_LISTEN_PORT);
	/* server main loop */
	while (1) {
		struct epoll_event rev;

		int rc = epoll_wait(epollfd, &rev, 1, -1);

		DIE(rc < 0, "epoll_wait");

		if (rev.data.fd == listenfd)
			handle_new_connection();

		else
			handle_client(rev.events, rev.data.ptr);
	}

	return 0;
}
