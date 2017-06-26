package main

import (
	"context"
	"flag"
	"fmt"
	"os"

	mgo "gopkg.in/mgo.v2"

	"github.com/chop-dbhi/nats-rpc/log"
	"github.com/chop-dbhi/nats-rpc/transport"
	"github.com/nats-io/go-nats"
	"github.com/rdm-academy/service/account"

	"go.uber.org/zap"
)

const (
	svcType = "account"
)

var (
	buildVersion string
)

func main() {
	var (
		natsAddr     string
		mongoAddr    string
		printVersion bool
	)

	flag.StringVar(&natsAddr, "nats.addr", "nats://127.0.0.1:4222", "NATS address.")
	flag.StringVar(&mongoAddr, "mongo.addr", "127.0.0.1:27017/account", "Mongo database URI.")
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

	// Open a session.
	session, err := mgo.Dial(mongoAddr)
	if err != nil {
		log.Fatal(err)
	}
	defer session.Close()

	// Default database for address.
	db := session.DB("")

	// Initialize the service.
	svc, err := account.NewService(db)
	if err != nil {
		log.Fatal(err)
	}

	logger.Info("serving `svc.account`")

	// Serve the service.
	ctx := context.Background()

	srv := account.NewServiceServer(tp, svc)
	if err := srv.Serve(ctx); err != nil {
		logger.Error("serve error", zap.Error(err))
		os.Exit(1)
	}
}
