# Asynchronous Web Server

Implemented a web server that uses advanced I/O operations.    

The server implements a limited functionality of the HTTP protocol: passing files to clients.

The web server will use the multiplexing API to wait for connections from clients - [epoll](https://man7.org/linux/man-pages/man7/epoll.7.html).
On the established connections, requests from clients will be received and then responses will be distributed to them.

The server will serve files from the `AWS_DOCUMENT_ROOT` directory, defined within the assignments' [header](./skel/aws.h).
Files are only found in subdirectories `AWS_DOCUMENT_ROOT/static/` and `AWS_DOCUMENT_ROOT/dynamic/`.
The corresponding request paths will be, for example, `AWS_DOCUMENT_ROOT/static/test.dat` and `AWS_DOCUMENT_ROOT/dynamic/test.dat`.
The file processing will be:

- The files in the `AWS_DOCUMENT_ROOT/static/` directory are static files that will be transmitted to clients using the zero-copying API - [sendfile](https://man7.org/linux/man-pages/man2/sendfile.2.html)]
- Files in the `AWS_DOCUMENT_ROOT/dynamic/` directory are files that are supposed to require a server-side post-processing phase. These files will be read from disk using the asynchronous API and then pushed to the clients. Streaming will use non-blocking sockets (Linux)
- An [HTTP 404](https://en.wikipedia.org/wiki/HTTP_404) message will be sent for invalid request paths

After transmitting a file, according to the HTTP protocol, the connection is closed.

### HTTP Parser

The clients and server will communicate using the HTTP protocol.
For parsing HTTP requests from clients we recommend using [this HTTP parser](https://github.com/nodejs/http-parser), also available in the assignments' [http-parser](./skel/http-parser).
You will need to use a callback to get the path to the local resource requested by the client.
Find a simplified example of using the parser in the [samples directory](./skel/http-parser/samples/).
