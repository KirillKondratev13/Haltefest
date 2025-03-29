package service

import (
	"context"
	"fmt"
	"time" // Добавляем импорт пакета time

	"github.com/jackc/pgx/v5/pgxpool"
)

type User struct {
    ID           int
    Username     string
    Email        string
    PasswordHash string
    CreatedAt    time.Time // Изменяем тип на time.Time
}

type UserService struct {
    DB *pgxpool.Pool
}

func (s *UserService) CreateUser(ctx context.Context, username, email, passwordHash string) (*User, error) {
    // Проверяем, существует ли пользователь с таким email
    var exists bool
    err := s.DB.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM users WHERE email = $1)", email).Scan(&exists)
    if err != nil {
        return nil, err
        // slog.Error("failed to create user", "error", err, "email", email)
        // return nil, fmt.Errorf("create user: %w", err)
    }
    if exists {
        return nil, fmt.Errorf("user with email %s already exists", email)
    }

    // Создаем нового пользователя
    var user User
    err = s.DB.QueryRow(ctx,
        "INSERT INTO users (username, email, password_hash) VALUES ($1, $2, $3) RETURNING id, username, email, created_at",
        username, email, passwordHash,
    ).Scan(&user.ID, &user.Username, &user.Email, &user.CreatedAt)
    if err != nil {
        return nil, err
    }
    return &user, nil
}

