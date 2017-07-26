package main

import (
	"context"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/rdm-academy/api/account"
	"github.com/rdm-academy/api/project"
)

type Enricher struct {
	accountSvc account.ServiceClient
	projectSvc project.ServiceClient
}

func (e *Enricher) GetUserName(ctx context.Context, id string) (string, error) {
	if id == "" {
		return "", nil
	}

	rep, err := e.accountSvc.GetUser(ctx, &account.GetUserRequest{
		Id: id,
	})

	if err != nil {
		return "", err
	}

	return rep.Name, nil
}

func (e *Enricher) CanViewProject(ctx context.Context, id, account string) (bool, error) {
	_, err := e.projectSvc.GetProject(ctx, &project.GetProjectRequest{
		Account: account,
		Id:      id,
	})

	if err != nil {
		if s, ok := status.FromError(err); ok {
			switch s.Code() {
			case codes.NotFound, codes.PermissionDenied, codes.Unauthenticated:
				return false, nil
			}
		}

		return false, err
	}

	return true, nil
}
