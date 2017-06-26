package main

import (
	"net/http"

	"github.com/labstack/echo"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var grpcHttpMap = map[codes.Code]int{
	codes.OK:                 http.StatusOK,
	codes.Canceled:           499, // client closed request
	codes.Unknown:            http.StatusInternalServerError,
	codes.InvalidArgument:    http.StatusBadRequest,
	codes.DeadlineExceeded:   http.StatusGatewayTimeout,
	codes.NotFound:           http.StatusNotFound,
	codes.AlreadyExists:      http.StatusConflict,
	codes.PermissionDenied:   http.StatusForbidden,
	codes.Unauthenticated:    http.StatusUnauthorized,
	codes.ResourceExhausted:  http.StatusTooManyRequests,
	codes.FailedPrecondition: http.StatusBadRequest,
	codes.Aborted:            http.StatusConflict,
	codes.OutOfRange:         http.StatusBadRequest,
	codes.Unimplemented:      http.StatusNotImplemented,
	codes.Internal:           http.StatusInternalServerError,
	codes.Unavailable:        http.StatusServiceUnavailable,
	codes.DataLoss:           http.StatusInternalServerError,
}

// grpcHTTPErrorHandler maps gRPC codes to HTTP codes.
func grpcHTTPErrorHandler(e *echo.Echo) echo.HTTPErrorHandler {
	handleError := e.HTTPErrorHandler

	return func(err error, c echo.Context) {
		if sts, ok := status.FromError(err); ok {
			httpCode := grpcHttpMap[sts.Code()]
			msg := sts.Message()

			// No body.
			if msg == "" {
				if err := c.NoContent(httpCode); err != nil {
					e.Logger.Error(err)
				}
				return
			}

			err = echo.NewHTTPError(httpCode, msg)
		}

		handleError(err, c)
	}
}
