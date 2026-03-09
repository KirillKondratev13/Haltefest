package storage

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
)

type DBClient struct {
	conn *pgx.Conn
}

func NewDBClient(connStr string) (*DBClient, error) {
	conn, err := pgx.Connect(context.Background(), connStr)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to postgres: %w", err)
	}
	return &DBClient{conn: conn}, nil
}

func (d *DBClient) ReportError(ctx context.Context, fileID int, errMsg string) error {
	_, err := d.conn.Exec(ctx, "UPDATE files SET status = 'ERROR', error_msg = $1, failure_cause = $1 WHERE id = $2", errMsg, fileID)
	return err
}

func (d *DBClient) Close() {
	d.conn.Close(context.Background())
}
