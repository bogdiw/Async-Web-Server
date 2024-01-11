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
			"HTTP/1.1 200 OK\r\nConnection: closed\r\n\r\n");
	conn->send_len = strlen(conn->send_buffer);
}

static void connection_prepare_send_404(struct connection *conn)
{
	/* TODO: Prepare the connection buffer to send the 404 header. */
	memset(conn->send_buffer, 0, sizeof(conn->send_buffer));
    sprintf(conn->send_buffer,
            "HTTP/1.1 404 Not Found\r\nConnection: closed\r\n\r\n");
    conn->send_len = strlen(conn->send_buffer);

}

static enum resource_type connection_get_resource_type(struct connection *conn)
{
	/* TODO: Get resource type depending on request path/filename. Filename should
	 * point to the static or dynamic folder.
	 */

	dlog(LOG_DEBUG, "Filename: %s\n", conn->request_path);
	if (!conn->have_path) {
        return RESOURCE_TYPE_NONE;
    }
	if (strstr(conn->request_path, "static") != NULL) {
		return RESOURCE_TYPE_STATIC;
	}

	if (strstr(conn->request_path, "dynamic") != NULL) {
		return RESOURCE_TYPE_DYNAMIC;
	}
    return RESOURCE_TYPE_NONE;
}


struct connection *connection_create(int sockfd)
{
	/* TODO: Initialize connection structure on given socket. */

	struct connection *conn = malloc(sizeof(*conn));
	
	DIE(conn == NULL, "malloc");

	conn->sockfd = sockfd;
	memset(conn->recv_buffer, 0, BUFSIZ);
	memset(conn->send_buffer, 0, BUFSIZ);
	conn->recv_len = 0;
	conn->send_len = 0;
	conn->send_pos = 0;
	conn->fd = -1;
	conn->file_size = 0;
	conn->file_pos = 0;
	conn->state = STATE_INITIAL;
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
	dlog(LOG_DEBUG, "Removing connection\n");
	w_epoll_remove_ptr(epollfd, conn->sockfd, conn);
	close(conn->sockfd);
	conn->state = STATE_NO_STATE;
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
	
	int sockfd;
	socklen_t addrlen = sizeof(struct sockaddr_in);
	struct sockaddr_in addr;

	/* accept new connection */
	sockfd = accept(listenfd, (SSA *) &addr, &addrlen);
	DIE(sockfd < 0, "accept");

	dlog(LOG_INFO, "Accepted connection from %s:%d\n",
		inet_ntoa(addr.sin_addr), ntohs(addr.sin_port));

	/* instantiate new connection handler */
	struct connection *conn = connection_create(sockfd);

	/* add socket to epoll */
	int rc = w_epoll_add_ptr_in(epollfd, sockfd, conn);
	DIE(rc < 0, "w_epoll_add_fd_in");

}

void receive_data(struct connection *conn)
{
	ssize_t bytes_recv;
	char abuffer[64];
	int rc;

	rc = get_peer_address(conn->sockfd, abuffer, 64);
	if (rc < 0) {
		ERR("get_peer_address");
		rc = w_epoll_remove_ptr(epollfd, conn->sockfd, conn);
		DIE(rc < 0, "w_epoll_remove_ptr");
		connection_remove(conn);
		return;
	}
	while (strstr(conn->recv_buffer, "\r\n\r\n") == NULL) {
		bytes_recv = recv(conn->sockfd, conn->recv_buffer + conn->recv_len, BUFSIZ, 0);
		if (bytes_recv < 0) {
			dlog(LOG_ERR, "Error in communication from: %s\n", abuffer);
			rc = w_epoll_remove_ptr(epollfd, conn->sockfd, conn);
			DIE(rc < 0, "w_epoll_remove_ptr");
			connection_remove(conn);
			return;
		}
		if (bytes_recv == 0) {
			dlog(LOG_INFO, "Connection closed to %s\n", abuffer);
			rc = w_epoll_remove_ptr(epollfd, conn->sockfd, conn);
			DIE(rc < 0, "w_epoll_remove_ptr");
			connection_remove(conn);
			return;
		}
		conn->recv_len += bytes_recv;
	}
	dlog(LOG_DEBUG, "Received %s\n", conn->recv_buffer);
	printf("--\n%s--\n", conn->recv_buffer);

	parse_header(conn);
	dlog(LOG_DEBUG, "FILENAME: %s\n", conn->request_path);
	rc = connection_open_file(conn);
	if (rc < 0) {
		ERR("connection_open_file");
		return -1;
	}

	conn->res_type = connection_get_resource_type(conn);
	conn->state = STATE_REQUEST_RECEIVED;
	rc = w_epoll_update_ptr_inout(epollfd, conn->sockfd, conn);
	DIE(rc < 0, "w_epoll_update_ptr_inout");
	return;
}

int connection_open_file(struct connection *conn)
{
	/* TODO: Open file and update connection fields. */
	if (conn == NULL) {
		return -1;
	}
	conn->fd = open(conn->request_path, O_RDWR);
	dlog(LOG_DEBUG, "Opening file %s\n", conn->request_path);
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
	http_parser_init(&conn->request_parser, HTTP_REQUEST);
	conn->request_parser.data = conn;
	http_parser_execute(&conn->request_parser, &settings_on_path, conn->recv_buffer, conn->recv_len);
	memmove(conn->request_path, conn->request_path + 1, strlen(conn->request_path));
	conn->have_path = 1;
	int rc = w_epoll_update_ptr_inout(epollfd, conn->sockfd, conn);
	DIE(rc < 0, "w_epoll_update_ptr_inout");
	return 0;
}

enum connection_state connection_send_static(struct connection *conn)
{
	/* TODO: Send static data using sendfile(2). */
	ssize_t bytes_sent;
	char abuffer[64];
	int rc;

	rc = get_peer_address(conn->sockfd, abuffer, 64);
	if (rc < 0) {
		ERR("get_peer_address");
		rc = w_epoll_remove_ptr(epollfd, conn->sockfd, conn);
		DIE(rc < 0, "w_epoll_remove_ptr");
		connection_remove(conn);
		return STATE_CONNECTION_CLOSED;
	}
	// send the file
	if (conn->fd != -1) {
		while (conn->file_size > 0) {
			rc = sendfile(conn->sockfd, conn->fd, NULL, conn->file_size);
			DIE(rc < 0, "sendfile");
			conn->file_size -= rc;
		}
	}

	conn->state = STATE_DATA_SENT;
	return STATE_DATA_SENT;
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
	ssize_t bytes_sent = 0;
	int rc;
	char abuffer[64];

	rc = get_peer_address(conn->sockfd, abuffer, 64);
	if (rc < 0) {
		ERR("get_peer_address");
	}
	
	while (conn->send_len > 0) {
		bytes_sent = send(conn->sockfd, conn->send_buffer + conn->send_pos, conn->send_len, 0);

		conn->send_len -= bytes_sent;
		conn->send_pos += bytes_sent;

		printf("--\n%s--\n", conn->send_buffer);

	}
	dlog(LOG_DEBUG, "Sent %s\n", conn->send_buffer);
	conn->state = STATE_HEADER_SENT;
	return 0;
}


int connection_send_dynamic(struct connection *conn)
{
	/* TODO: Read data asynchronously.
	 * Returns 0 on success and -1 on error.
	 */
	if (conn == NULL) {
		return -1;
	}
	connection_start_async_io(conn);
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
		conn->state = STATE_RECEIVING_DATA;
		break;
	case STATE_RECEIVING_DATA:
		receive_data(conn);
		conn->state = STATE_SENDING_HEADER;
		break;
	case STATE_ASYNC_ONGOING:
		connection_complete_async_io(conn);
		break;
	case STATE_SENDING_404:
		connection_send_data(conn);
		if (conn->send_pos >= conn->send_len) {
			conn->state = STATE_CONNECTION_CLOSED;
		}
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
		if (conn->res_type == RESOURCE_TYPE_NONE) {
			connection_prepare_send_404(conn);
			dlog(LOG_DEBUG, "Preparing 404\n");
		} else if (conn->res_type == RESOURCE_TYPE_STATIC) {
			connection_prepare_send_reply_header(conn);
			dlog(LOG_DEBUG, "Preparing header\n");
		} else if (conn->res_type == RESOURCE_TYPE_DYNAMIC) {
			connection_prepare_send_reply_header(conn);
			dlog(LOG_DEBUG, "Preparin header2\n");
		}
		connection_send_data(conn);
		if (conn->res_type == RESOURCE_TYPE_NONE) {
			conn->state = STATE_404_SENT;
		} else if (conn->res_type == RESOURCE_TYPE_STATIC || conn->res_type == RESOURCE_TYPE_DYNAMIC) {
			conn->state = STATE_SENDING_DATA;
		}
		dlog(LOG_DEBUG, "HEADER SENT\n");
		break;
	case STATE_SENDING_DATA:
		dlog(LOG_DEBUG, "Sending data for real\n");
		if (conn->res_type == RESOURCE_TYPE_STATIC) {
			dlog(LOG_DEBUG, "Sending static\n");
			connection_send_static(conn);
		} else if (conn->res_type == RESOURCE_TYPE_DYNAMIC) {
			connection_send_dynamic(conn);
		}
		conn->state = STATE_CONNECTION_CLOSED;
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
	case STATE_NO_STATE:
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
	int rc;

	/* TODO: Initialize asynchronous operations. */

	/* TODO: Initialize multiplexing. */
	epollfd = w_epoll_create();
	DIE(epollfd < 0, "w_epoll_create");
	/* TODO: Create server socket. */
	listenfd = tcp_create_listener(AWS_LISTEN_PORT, 
		DEFAULT_LISTEN_BACKLOG);
	DIE(listenfd < 0, "socket");
	/* TODO: Add server socket to epoll object*/

	rc = w_epoll_add_fd_in(epollfd, listenfd);
	DIE(rc < 0, "w_epoll_add_fd_in");
	
	/* Uncomment the following line for debugging. */
	dlog(LOG_INFO, "Server waiting for connections on port %d\n", AWS_LISTEN_PORT);

	/* server main loop */
	while (1) {
		struct epoll_event rev;

		/* TODO: Wait for events. */
		rc = w_epoll_wait_infinite(epollfd, &rev);
		DIE(rc < 0, "w_epoll_wait_infinite");
		/* TODO: Switch event types; consider
		 *   - new connection requests (on server socket)
		 *   - socket communication (on connection sockets)
		 */
		if (rev.data.fd == listenfd) {
			dlog(LOG_DEBUG, "New connection\n");
			if (rev.events & EPOLLIN)
				handle_new_connection();
		} else {
			struct connection *conn = (struct connection *)rev.data.ptr;
			if (rev.events & EPOLLIN) {
				dlog(LOG_DEBUG, "New message\n");
				handle_client(EPOLLIN, conn);
			}
			if (rev.events & EPOLLOUT) {
				dlog(LOG_DEBUG, "Ready to send message\n");
				handle_client(EPOLLOUT, conn);
			}
		}
	}

	return 0;
}
