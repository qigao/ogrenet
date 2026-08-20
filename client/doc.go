// Package client provides production-oriented client transports that preserve
// their native protocol semantics. HTTP/1.1, HTTP/2, and HTTP/3 use net/http
// request, response, pooling, and cancellation behavior rather than
// transport.Session.
package client
