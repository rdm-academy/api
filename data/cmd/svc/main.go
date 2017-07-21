package main

import (
	"context"
	"flag"
	"fmt"
	"os"

	mgo "gopkg.in/mgo.v2"

	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/chop-dbhi/nats-rpc/log"
	"github.com/chop-dbhi/nats-rpc/transport"
	"github.com/nats-io/go-nats"
	"github.com/rdm-academy/api/data"
	"github.com/rdm-academy/api/storage"
	"github.com/rdm-academy/api/storage/aws"
	"github.com/rdm-academy/api/storage/gcp"

	"go.uber.org/zap"
)

const (
	svcType = "data"
)

var (
	buildVersion string
)

func main() {
	var (
		natsAddr  string
		mongoAddr string

		bucketName string

		awsAccessKey    string
		awsSecretKey    string
		awsSessionToken string
		awsRegion       string
		awsProfile      string
		awsSSE          string

		gcpProjectId          string
		gcpServiceAccountFile string

		printVersion bool
	)

	flag.StringVar(&natsAddr, "nats.addr", "nats://127.0.0.1:4222", "NATS address.")
	flag.StringVar(&mongoAddr, "mongo.addr", "127.0.0.1:27017/data", "Mongo database URI.")

	flag.StringVar(&bucketName, "bucket", "", "Bucket name.")

	flag.StringVar(&awsAccessKey, "aws.access-key", "", "AWS access key.")
	flag.StringVar(&awsSecretKey, "aws.secret-key", "", "AWS secret key.")
	flag.StringVar(&awsSessionToken, "aws.session-token", "", "AWS token.")
	flag.StringVar(&awsRegion, "aws.region", "us-east-1", "AWS region.")
	flag.StringVar(&awsProfile, "aws.profile", "", "AWS profile.")
	flag.StringVar(&awsSSE, "aws.sse", "", "AWS server-side encryption.")

	flag.StringVar(&gcpProjectId, "gcp.project", "", "GCP project id")
	flag.StringVar(&gcpServiceAccountFile, "gcp.service-account", "", "GCP service account file")

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

	ctx := context.Background()

	var stg storage.Storage

	// AWS S3 or Minio.
	if awsProfile != "" {
		stg, err = aws.NewStorage(aws.Config{
			Region:               awsRegion,
			ServerSideEncryption: awsSSE,
			Credentials:          credentials.NewSharedCredentials("", awsProfile),
		})
	} else if awsSecretKey != "" {
		stg, err = aws.NewStorage(aws.Config{
			Region:               awsRegion,
			ServerSideEncryption: awsSSE,
			Credentials: credentials.NewStaticCredentialsFromCreds(credentials.Value{
				AccessKeyID:     awsAccessKey,
				SecretAccessKey: awsSecretKey,
				SessionToken:    awsSessionToken,
			}),
		})
		// GCP.
	} else if gcpProjectId != "" {
		stg, err = gcp.NewStorage(gcp.Config{
			Context:            ctx,
			Project:            gcpProjectId,
			ServiceAccountFile: gcpServiceAccountFile,
		})
	}
	if err != nil {
		log.Fatal(err)
	}
	if stg == nil {
		log.Fatal("storage options must be set")
	}
	defer stg.Close()

	// Open a session.
	session, err := mgo.Dial(mongoAddr)
	if err != nil {
		log.Fatal(err)
	}
	defer session.Close()

	// Default database for address.
	db := session.DB("")

	// Initialize the service.
	svc, err := data.NewService(data.Config{
		DB:      db,
		Storage: stg,
		Bucket:  bucketName,
	})
	if err != nil {
		log.Fatal(err)
	}

	logger.Info("serving `svc.data`")

	// Serve the service.
	srv := data.NewServiceServer(tp, svc)
	if err := srv.Serve(ctx); err != nil {
		logger.Error("serve error", zap.Error(err))
		os.Exit(1)
	}
}
