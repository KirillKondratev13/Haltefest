package app

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"
)

type User struct {
    ID           int
    Username     string
    Email        string
    PasswordHash string
    CreatedAt    string
}

type UserService struct {
    DB *pgxpool.Pool
}

func (s *UserService) CreateUser(ctx context.Context, username, email, passwordHash string) (*User, error) {
    var user User
    err := s.DB.QueryRow(ctx,
        "INSERT INTO users (username, email, password_hash) VALUES ($1, $2, $3) RETURNING id, username, email, created_at",
        username, email, passwordHash,
    ).Scan(&user.ID, &user.Username, &user.Email, &user.CreatedAt)
    if err != nil {
        return nil, err
    }
    return &user, nil
}