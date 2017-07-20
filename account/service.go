package account

import (
	"context"
	"strings"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	mgo "gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"

	uuid "github.com/satori/go.uuid"
)

const (
	usersCol = "users"
)

type Identity struct {
	Subject string    `bson:"subject"`
	Email   string    `bson:"email"`
	Name    string    `bson:"name"`
	Issuer  string    `bson:"issuer"`
	Created time.Time `bson:"created"`
}

type User struct {
	ID         string      `bson:"_id"`
	Email      string      `bson:"email"`
	Name       string      `bson:"name"`
	Identities []*Identity `bson:"identities"`
	Created    time.Time   `bson:"created"`
	Modified   time.Time   `bson:"modified"`
}

type service struct {
	db *mgo.Database
}

func (s *service) CreateUser(ctx context.Context, req *CreateUserRequest) (*CreateUserResponse, error) {
	if req.Email == "" {
		return nil, status.Error(codes.InvalidArgument, "email required")
	}

	now := time.Now()

	// Identifier.
	email := strings.ToLower(req.Email)

	user := User{
		ID:       uuid.NewV4().String(),
		Email:    email,
		Name:     req.Name,
		Created:  now,
		Modified: now,
		Identities: []*Identity{
			&Identity{
				Subject: req.Subject,
				Email:   email,
				Name:    req.Name,
				Issuer:  req.Issuer,
				Created: now,
			},
		},
	}

	if err := s.db.C(usersCol).Insert(&user); err != nil {
		// Already exists by ID or email.
		if mgo.IsDup(err) {
			return nil, status.Error(codes.AlreadyExists, "user already exists")
		}

		// Unknown.
		return nil, err
	}

	rep := &CreateUserResponse{
		Id:    user.ID,
		Email: email,
		Name:  user.Name,
	}

	return rep, nil
}

func (s *service) GetUser(ctx context.Context, req *GetUserRequest) (*GetUserResponse, error) {
	var query bson.M

	if req.Id != "" {
		query = bson.M{
			"_id": req.Id,
		}
	} else if req.Email != "" {
		query = bson.M{
			"email": strings.ToLower(req.Email),
		}
	} else {
		return nil, status.Error(codes.InvalidArgument, "id or email must be specified")
	}

	var user User
	if err := s.db.C(usersCol).Find(query).One(&user); err != nil {
		if err == mgo.ErrNotFound {
			return nil, status.Error(codes.NotFound, "user does not exist")
		}

		return nil, err
	}

	return &GetUserResponse{
		Id:    user.ID,
		Email: user.Email,
		Name:  user.Name,
	}, nil
}

func (s *service) DeleteUser(ctx context.Context, req *DeleteUserRequest) (*DeleteUserResponse, error) {
	if req.Id == "" {
		return nil, status.Error(codes.InvalidArgument, "user id required")
	}

	if err := s.db.C(usersCol).RemoveId(req.Id); err != nil {
		if err == mgo.ErrNotFound {
			return nil, status.Error(codes.NotFound, "user not found")
		}

		return nil, err
	}

	return &DeleteUserResponse{}, nil
}

func NewService(db *mgo.Database) (Service, error) {
	err := db.C(usersCol).EnsureIndex(mgo.Index{
		Key:    []string{"email"},
		Unique: true,
	})
	if err != nil {
		return nil, err
	}

	return &service{
		db: db,
	}, nil
}
