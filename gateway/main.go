package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"strings"
	"time"

	"go.uber.org/zap"

	"github.com/chop-dbhi/nats-rpc/log"
	"github.com/chop-dbhi/nats-rpc/transport"
	jwt "github.com/dgrijalva/jwt-go"
	"github.com/labstack/echo"
	"github.com/labstack/echo/middleware"
	"github.com/nats-io/go-nats"
	"github.com/rdm-academy/service/account"
	"github.com/rdm-academy/service/project"
	"github.com/tylerb/graceful"
)

const (
	svcType = "gateway"
)

var (
	buildVersion string
)

func main() {
	var (
		httpAddr     string
		corsHosts    string
		natsAddr     string
		jwtKey       string
		printVersion bool
	)

	flag.StringVar(&httpAddr, "http.addr", "127.0.0.1:8080", "HTTP bind address.")
	flag.StringVar(&corsHosts, "cors.hosts", "", "List of CORS allowed hosts.")
	flag.StringVar(&natsAddr, "nats.addr", "nats://127.0.0.1:4222", "NATS address.")
	flag.StringVar(&jwtKey, "jwt.key", "", "JWT key.")
	flag.BoolVar(&printVersion, "version", false, "Print version.")

	flag.Parse()

	if printVersion {
		fmt.Fprintln(os.Stdout, buildVersion)
		return
	}

	// Initialize base logger.
	logger, err := log.New()
	if err != nil {
		log.Fatal(err)
	}

	logger = logger.With(
		zap.String("service.type", svcType),
		zap.String("service.version", buildVersion),
	)

	// Initialize the transport layer.
	tp, err := transport.Connect(&nats.Options{
		Url:            natsAddr,
		AllowReconnect: true,
		MaxReconnect:   -1,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer tp.Close()

	tp.SetLogger(logger)

	// Setup HTTP mux.
	e := echo.New()

	// Disable internal logger.
	e.Logger.SetOutput(ioutil.Discard)

	// Set custom error handler.
	e.HTTPErrorHandler = grpcHTTPErrorHandler(e)

	// Setup middleware.
	e.Pre(TraceMiddleware())
	e.Use(LoggingMiddlware(logger))
	e.Pre(middleware.RemoveTrailingSlash())
	e.Use(middleware.Recover())
	e.Use(middleware.Gzip())

	// If CORS hosts are specified, add middleware to restrict access.
	if corsHosts != "" {
		config := middleware.CORSConfig{
			AllowOrigins:     strings.Split(corsHosts, ","),
			AllowCredentials: true,
			AllowHeaders: []string{
				"Authorization",
				"Content-Type",
			},
		}

		e.Use(middleware.CORSWithConfig(config))
	}

	//
	// Routes.
	//

	// Endpoint to check if the service is healthy (e.g readiness probe).
	e.GET("/healthz", func(c echo.Context) error {
		return c.NoContent(http.StatusOK)
	})

	// Endpoint to check if the service is ready (e.g. liveness probe).
	e.GET("/readyz", func(c echo.Context) error {
		if tp.Conn().IsConnected() {
			return c.NoContent(http.StatusOK)
		}
		return c.NoContent(http.StatusServiceUnavailable)
	})

	//
	// Protected routes.
	//

	// Ensure a valid JWT is present.
	authMiddleware := middleware.JWTWithConfig(middleware.JWTConfig{
		SigningKey: []byte(jwtKey),
	})

	// Account endpoints.
	accounts := account.NewServiceClient(tp)

	// Ensure the user account is created. This requires a valid JWT token
	// and extracts the profile information out.
	// If the account already exists for the email address, this is a no-op.
	e.PUT("/account", func(c echo.Context) error {
		token, _ := c.Get("user").(*jwt.Token)
		claims, _ := token.Claims.(jwt.MapClaims)

		// Get values.
		email, _ := claims["email"].(string)

		ctx := c.Request().Context()

		req := &account.GetUserRequest{
			Email: email,
		}

		if rep, err := accounts.GetUser(ctx, req); err == nil {
			return c.JSON(http.StatusOK, rep)
		}

		subject, _ := claims["sub"].(string)
		issuer, _ := claims["iss"].(string)
		name, _ := claims["name"].(string)

		req2 := &account.CreateUserRequest{
			Issuer:  issuer,
			Subject: subject,
			Name:    name,
			Email:   email,
		}

		rep, err := accounts.CreateUser(ctx, req2)
		if err != nil {
			return err
		}

		return c.JSON(http.StatusCreated, rep)
	}, authMiddleware)

	// Adds information of an authenticated user to the request context.
	// This must come after the authMiddleware.
	userMiddleware := AccountMiddleware(accounts)

	// Account of the requesting user.
	e.GET("/account", func(c echo.Context) error {
		req := account.GetUserRequest{
			Id: c.Get("user.id").(string),
		}

		rep, err := accounts.GetUser(c.Request().Context(), &req)
		if err != nil {
			return err
		}

		return c.JSON(http.StatusOK, rep)
	}, authMiddleware, userMiddleware)

	// Account of the requesting user.
	e.DELETE("/account", func(c echo.Context) error {
		req := account.DeleteUserRequest{
			Id: c.Get("user.id").(string),
		}

		_, err := accounts.DeleteUser(c.Request().Context(), &req)
		if err != nil {
			return err
		}

		return c.NoContent(http.StatusOK)
	}, authMiddleware, userMiddleware)

	projects := project.NewServiceClient(tp)

	// Project endpoints.
	e.POST("/projects", func(c echo.Context) error {
		var req project.CreateProjectRequest
		if err := c.Bind(&req); err != nil {
			return c.JSON(http.StatusUnprocessableEntity, err)
		}

		req.Account = c.Get("user.id").(string)

		rep, err := projects.CreateProject(c.Request().Context(), &req)
		if err != nil {
			return err
		}

		return c.JSON(http.StatusCreated, rep.Project)
	}, authMiddleware, userMiddleware)

	e.PUT("/projects/:id", func(c echo.Context) error {
		var req project.UpdateProjectRequest
		if err := c.Bind(&req); err != nil {
			return c.JSON(http.StatusUnprocessableEntity, err)
		}

		req.Id = c.Param("id")
		req.Account = c.Get("user.id").(string)

		_, err := projects.UpdateProject(c.Request().Context(), &req)
		if err != nil {
			return err
		}

		return c.NoContent(http.StatusOK)
	}, authMiddleware, userMiddleware)

	e.DELETE("/projects/:id", func(c echo.Context) error {
		req := &project.DeleteProjectRequest{
			Id:      c.Param("id"),
			Account: c.Get("user.id").(string),
		}

		_, err := projects.DeleteProject(c.Request().Context(), req)
		if err != nil {
			return err
		}

		return c.NoContent(http.StatusOK)
	}, authMiddleware, userMiddleware)

	e.GET("/projects", func(c echo.Context) error {
		req := project.ListProjectsRequest{
			Account: c.Get("user.id").(string),
		}

		rep, err := projects.ListProjects(c.Request().Context(), &req)
		if err != nil {
			return err
		}

		return c.JSON(http.StatusOK, rep.Projects)
	}, authMiddleware, userMiddleware)

	// Gracefully serve.
	logger.Info("listening",
		zap.String("http.addr", httpAddr),
	)

	e.Server.Addr = httpAddr
	if err := graceful.ListenAndServe(e.Server, 5*time.Second); err != nil {
		logger.Error("http error",
			zap.Error(err),
		)
	}
}
