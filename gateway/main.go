package main

import (
	"encoding/json"
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
	"github.com/rdm-academy/api/account"
	"github.com/rdm-academy/api/commitlog"
	"github.com/rdm-academy/api/data"
	"github.com/rdm-academy/api/nodes"
	"github.com/rdm-academy/api/project"
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

	// Service clients.
	accountSvc := account.NewServiceClient(tp)
	projectSvc := project.NewServiceClient(tp)
	commitlogSvc := commitlog.NewServiceClient(tp)
	dataSvc := data.NewServiceClient(tp)
	nodeSvc := nodes.NewServiceClient(tp)

	// Used to enrich objects prior to get them to the client.
	// A poor mans GraphQL..
	enricher := &Enricher{
		accountSvc: accountSvc,
		projectSvc: projectSvc,
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

		if rep, err := accountSvc.GetUser(ctx, req); err == nil {
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

		rep, err := accountSvc.CreateUser(ctx, req2)
		if err != nil {
			return err
		}

		return c.JSON(http.StatusCreated, rep)
	}, authMiddleware)

	// Adds information of an authenticated user to the request context.
	// This must come after the authMiddleware.
	userMiddleware := AccountMiddleware(accountSvc)

	// Account of the requesting user.
	e.GET("/account", func(c echo.Context) error {
		req := account.GetUserRequest{
			Id: c.Get("user.id").(string),
		}

		rep, err := accountSvc.GetUser(c.Request().Context(), &req)
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

		_, err := accountSvc.DeleteUser(c.Request().Context(), &req)
		if err != nil {
			return err
		}

		return c.NoContent(http.StatusOK)
	}, authMiddleware, userMiddleware)

	// Project endpoints.
	e.POST("/projects", func(c echo.Context) error {
		var req project.CreateProjectRequest
		if err := c.Bind(&req); err != nil {
			return c.JSON(http.StatusUnprocessableEntity, err)
		}

		req.Account = c.Get("user.id").(string)

		rep, err := projectSvc.CreateProject(c.Request().Context(), &req)
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

		_, err := projectSvc.UpdateProject(c.Request().Context(), &req)
		if err != nil {
			return err
		}

		return c.NoContent(http.StatusOK)
	}, authMiddleware, userMiddleware)

	e.PUT("/projects/:id/workflow", func(c echo.Context) error {
		var req project.UpdateWorkflowRequest
		if err := c.Bind(&req); err != nil {
			return c.JSON(http.StatusUnprocessableEntity, err)
		}

		req.Id = c.Param("id")
		req.Account = c.Get("user.id").(string)

		_, err := projectSvc.UpdateWorkflow(c.Request().Context(), &req)
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

		_, err := projectSvc.DeleteProject(c.Request().Context(), req)
		if err != nil {
			return err
		}

		return c.NoContent(http.StatusOK)
	}, authMiddleware, userMiddleware)

	e.GET("/projects", func(c echo.Context) error {
		req := project.ListProjectsRequest{
			Account: c.Get("user.id").(string),
		}

		rep, err := projectSvc.ListProjects(c.Request().Context(), &req)
		if err != nil {
			return err
		}

		return c.JSON(http.StatusOK, rep.Projects)
	}, authMiddleware, userMiddleware)

	e.GET("/projects/:id", func(c echo.Context) error {
		req := project.GetProjectRequest{
			Id:      c.Param("id"),
			Account: c.Get("user.id").(string),
		}

		rep, err := projectSvc.GetProject(c.Request().Context(), &req)
		if err != nil {
			return err
		}

		return c.JSON(http.StatusOK, rep.Project)
	}, authMiddleware, userMiddleware)

	type apiEvent struct {
		ID     string          `json:"id"`
		Time   int64           `json:"time"`
		Type   string          `json:"type"`
		Author string          `json:"author"`
		Data   json.RawMessage `json:"data"`
	}

	type apiCommit struct {
		ID     string      `json:"id"`
		Msg    string      `json:"msg"`
		Author string      `json:"author"`
		Time   int64       `json:"time"`
		Events []*apiEvent `json:"events"`
		Parent string      `json:"parent"`
	}

	e.GET("/projects/:id/log", func(c echo.Context) error {
		ctx := c.Request().Context()
		account := c.Get("user.id").(string)

		canView, err := enricher.CanViewProject(ctx, c.Param("id"), account)
		if err != nil {
			return c.String(http.StatusServiceUnavailable, err.Error())
		}

		if !canView {
			return c.NoContent(http.StatusNotFound)
		}

		req := commitlog.HistoryRequest{
			Project: c.Param("id"),
		}

		commits := make([]*apiCommit, 0)
		authors := make(map[string]string)

		for {
			rep, err := commitlogSvc.History(ctx, &req)
			if err != nil {
				return err
			}

			if rep.Commit == nil {
				break
			}

			events := make([]*apiEvent, len(rep.Commit.Events))
			for i, e := range rep.Commit.Events {
				var author string
				if x, ok := authors[e.Author]; ok {
					author = x
				} else {
					author, _ = enricher.GetUserName(ctx, e.Author)
					authors[e.Author] = author
				}

				events[i] = &apiEvent{
					ID:     e.Id,
					Time:   e.Time,
					Type:   e.Type,
					Author: author,
					Data:   json.RawMessage(e.Data),
				}
			}

			var author string
			if x, ok := authors[rep.Commit.Author]; ok {
				author = x
			} else {
				author, _ = enricher.GetUserName(ctx, rep.Commit.Author)
				authors[rep.Commit.Author] = author
			}

			commits = append(commits, &apiCommit{
				ID:     rep.Commit.Id,
				Msg:    rep.Commit.Msg,
				Author: author,
				Time:   rep.Commit.Time,
				Parent: rep.Commit.Parent,
				Events: events,
			})

			// Any more?
			if rep.Next == "" {
				break
			}

			req.Commit = rep.Next
		}

		return c.JSON(http.StatusOK, commits)
	}, authMiddleware, userMiddleware)

	e.GET("/projects/:id/log/pending", func(c echo.Context) error {
		ctx := c.Request().Context()
		account := c.Get("user.id").(string)

		canView, err := enricher.CanViewProject(ctx, c.Param("id"), account)
		if err != nil {
			return c.String(http.StatusServiceUnavailable, err.Error())
		}
		if !canView {
			return c.NoContent(http.StatusNotFound)
		}

		req := commitlog.PendingRequest{
			Project: c.Param("id"),
		}

		rep, err := commitlogSvc.Pending(ctx, &req)
		if err != nil {
			return err
		}

		authors := make(map[string]string)
		events := make([]*apiEvent, len(rep.Events))

		for i, e := range rep.Events {
			var author string
			if x, ok := authors[e.Author]; ok {
				author = x
			} else {
				author, _ = enricher.GetUserName(ctx, e.Author)
				authors[e.Author] = author
			}

			events[i] = &apiEvent{
				ID:     e.Id,
				Time:   e.Time,
				Type:   e.Type,
				Author: author,
				Data:   json.RawMessage(e.Data),
			}
		}

		return c.JSON(http.StatusOK, events)
	}, authMiddleware, userMiddleware)

	e.POST("/projects/:id/log", func(c echo.Context) error {
		ctx := c.Request().Context()
		account := c.Get("user.id").(string)

		canView, err := enricher.CanViewProject(ctx, c.Param("id"), account)
		if err != nil {
			return c.String(http.StatusServiceUnavailable, err.Error())
		}

		if !canView {
			return c.NoContent(http.StatusNotFound)
		}

		var req commitlog.CommitRequest
		if err := c.Bind(&req); err != nil {
			return err
		}

		req.Project = c.Param("id")
		req.Author = account

		_, err = commitlogSvc.Commit(ctx, &req)
		if err != nil {
			return err
		}

		return c.NoContent(http.StatusOK)
	}, authMiddleware, userMiddleware)

	// Get data about a node.
	e.GET("/projects/:project/nodes/:node", func(c echo.Context) error {
		ctx := c.Request().Context()

		project := c.Param("project")
		node := c.Param("node")

		rep, err := nodeSvc.Get(ctx, &nodes.GetRequest{
			Project: project,
			Id:      node,
		})
		if err != nil {
			return err
		}

		return c.JSON(http.StatusOK, rep)
	}, authMiddleware, userMiddleware)

	type nodeData struct {
		Title *string
		Notes *string
	}

	// Update data about a node.
	e.PUT("/projects/:project/nodes/:node", func(c echo.Context) error {
		var data nodeData
		if err := c.Bind(&data); err != nil {
			return err
		}

		ctx := c.Request().Context()

		account := c.Get("user.id").(string)
		project := c.Param("project")
		node := c.Param("node")

		if data.Title != nil {
			_, err := nodeSvc.SetTitle(ctx, &nodes.SetTitleRequest{
				Project: project,
				Id:      node,
				Title:   *data.Title,
				Account: account,
			})
			if err != nil {
				return err
			}
		}

		if data.Notes != nil {
			_, err := nodeSvc.SetNotes(ctx, &nodes.SetNotesRequest{
				Project: project,
				Id:      node,
				Notes:   *data.Notes,
				Account: account,
			})
			if err != nil {
				return err
			}
		}

		return c.NoContent(http.StatusOK)
	}, authMiddleware, userMiddleware)

	// Upload files and associate it to the node.
	e.POST("/projects/:project/nodes/:node/upload", func(c echo.Context) error {
		ctx := c.Request().Context()

		account := c.Get("user.id").(string)
		project := c.Param("project")
		node := c.Param("node")

		_, err := nodeSvc.Get(ctx, &nodes.GetRequest{
			Project: project,
			Id:      node,
		})
		if err != nil {
			return err
		}

		form, err := c.MultipartForm()
		if err != nil {
			return err
		}

		var files []*nodes.File

		// TODO: parallelize.
		for _, f := range form.File["files"] {
			src, err := f.Open()
			if err != nil {
				return err
			}
			defer src.Close()

			// Use client helper function to upload.
			id, err := data.Upload(ctx, dataSvc, "", src)
			if err != nil {
				return err
			}

			files = append(files, &nodes.File{
				Id:   id,
				Name: f.Filename,
			})
		}

		_, err = nodeSvc.AddFiles(ctx, &nodes.AddFilesRequest{
			Project: project,
			Id:      node,
			Files:   files,
			Account: account,
		})
		if err != nil {
			return err
		}

		return c.NoContent(http.StatusOK)
	}, authMiddleware, userMiddleware)

	// Get details about a file.
	e.GET("/projects/:project/files/:file", func(c echo.Context) error {
		ctx := c.Request().Context()

		//project := c.Param("project")
		file := c.Param("file")

		rep, err := dataSvc.Describe(ctx, &data.DescribeRequest{
			Id: file,
		})
		if err != nil {
			return err
		}

		return c.JSON(http.StatusOK, rep)
	}, authMiddleware, userMiddleware)

	// Get a signed url for a client requested download.
	e.GET("/projects/:project/files/:file/url", func(c echo.Context) error {
		ctx := c.Request().Context()

		//project := c.Param("project")
		file := c.Param("file")

		rep, err := dataSvc.Get(ctx, &data.GetRequest{
			Id: file,
		})
		if err != nil {
			return err
		}

		return c.JSON(http.StatusOK, rep)
	}, authMiddleware, userMiddleware)

	// Download a file.
	e.GET("/projects/:project/files/:file/download", func(c echo.Context) error {
		ctx := c.Request().Context()

		// TODO: check association to project/permission
		//project := c.Param("project")
		file := c.Param("file")

		rep, err := dataSvc.Get(ctx, &data.GetRequest{
			Id: file,
		})
		if err != nil {
			return err
		}

		resp, err := http.Get(rep.SignedUrl)
		if err != nil {
			return err
		}
		defer resp.Body.Close()

		contentType := echo.MIMEOctetStream
		if rep.Mediatype != "" {
			contentType = rep.Mediatype
		}

		return c.Stream(http.StatusOK, contentType, resp.Body)
	}, authMiddleware, userMiddleware)

	// Remove a file from a node.
	e.DELETE("/projects/:project/nodes/:node/files/:file", func(c echo.Context) error {
		ctx := c.Request().Context()

		account := c.Get("user.id").(string)
		project := c.Param("project")
		node := c.Param("node")
		file := c.Param("file")

		_, err := nodeSvc.RemoveFiles(ctx, &nodes.RemoveFilesRequest{
			Project: project,
			Id:      node,
			Account: account,
			FileIds: []string{
				file,
			},
		})
		if err != nil {
			return err
		}

		return c.NoContent(http.StatusOK)
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
