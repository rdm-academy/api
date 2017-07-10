package main

import (
	"strconv"
	"time"

	"github.com/dgrijalva/jwt-go"
	"github.com/labstack/echo"
	"github.com/nats-io/nuid"
	"github.com/rdm-academy/api/account"
	"go.uber.org/zap"
)

var (
	traceIdKey   = "trace.id"
	userIdKey    = "user.id"
	userEmailKey = "user.email"
)

func TraceMiddleware() echo.MiddlewareFunc {
	return func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c echo.Context) error {
			c.Set(traceIdKey, nuid.Next())
			return next(c)
		}
	}
}

func AccountMiddleware(accounts account.ServiceClient) echo.MiddlewareFunc {
	return func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c echo.Context) error {
			if token, ok := c.Get("user").(*jwt.Token); ok {
				// Get the user's email address from the JWT
				var email string

				if claims, ok := token.Claims.(jwt.MapClaims); ok {
					email, _ = claims["email"].(string)
				}

				if email != "" {
					rep, err := accounts.GetUser(c.Request().Context(), &account.GetUserRequest{
						Email: email,
					})
					if err != nil {
						return err
					}

					c.Set(userIdKey, rep.Id)
					c.Set(userEmailKey, rep.Email)
				}
			}

			return next(c)
		}
	}
}

func LoggingMiddlware(logger *zap.Logger) echo.MiddlewareFunc {
	return func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c echo.Context) error {
			// References to request and response.
			req := c.Request()
			rep := c.Response()

			t0 := time.Now()

			// Call the next handler.
			err := next(c)
			if err != nil {
				// This sets the response.
				c.Error(err)
			}

			latency := float32(time.Now().Sub(t0)) / float32(time.Millisecond)

			// Get the request body size if present.
			reqSize, _ := strconv.ParseInt(req.Header.Get(echo.HeaderContentLength), 10, 0)

			traceId, _ := c.Get(traceIdKey).(string)
			userId, _ := c.Get(userIdKey).(string)

			logger.Info("request",
				zap.String("trace.id", traceId),
				zap.Int64("http.request_size", reqSize),
				zap.Int64("http.response_size", rep.Size),
				zap.String("http.method", req.Method),
				zap.String("http.path", req.URL.Path),
				zap.String("http.user_agent", req.UserAgent()),
				zap.Int("http.status", rep.Status),
				zap.Float32("latency.ms", latency),
				zap.String("user.id", userId),
			)

			return err
		}
	}
}
