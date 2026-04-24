package cache

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"
)

type DragonflyClient struct {
	addr     string
	password string
}

func NewDragonflyClient(addr string, password string) *DragonflyClient {
	return &DragonflyClient{
		addr:     strings.TrimSpace(addr),
		password: password,
	}
}

func (c *DragonflyClient) Get(ctx context.Context, key string) (string, bool, error) {
	reply, err := c.command(ctx, "GET", key)
	if err != nil {
		return "", false, err
	}
	value, ok := reply.(string)
	if !ok {
		return "", false, nil
	}
	return value, true, nil
}

func (c *DragonflyClient) Set(ctx context.Context, key string, value string, ttl time.Duration) error {
	seconds := int(ttl.Seconds())
	if seconds <= 0 {
		seconds = 1
	}
	_, err := c.command(ctx, "SET", key, value, "EX", strconv.Itoa(seconds))
	return err
}

func (c *DragonflyClient) command(ctx context.Context, args ...string) (interface{}, error) {
	if c == nil || c.addr == "" {
		return nil, fmt.Errorf("dragonfly address is empty")
	}

	dialer := &net.Dialer{Timeout: 2 * time.Second}
	conn, err := dialer.DialContext(ctx, "tcp", c.addr)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	if deadline, ok := ctx.Deadline(); ok {
		_ = conn.SetDeadline(deadline)
	} else {
		_ = conn.SetDeadline(time.Now().Add(3 * time.Second))
	}

	reader := bufio.NewReader(conn)
	writer := bufio.NewWriter(conn)

	if c.password != "" {
		if err := writeArray(writer, []string{"AUTH", c.password}); err != nil {
			return nil, err
		}
		if err := writer.Flush(); err != nil {
			return nil, err
		}
		if _, err := readReply(reader); err != nil {
			return nil, err
		}
	}

	if err := writeArray(writer, args); err != nil {
		return nil, err
	}
	if err := writer.Flush(); err != nil {
		return nil, err
	}
	return readReply(reader)
}

func writeArray(writer *bufio.Writer, args []string) error {
	if _, err := fmt.Fprintf(writer, "*%d\r\n", len(args)); err != nil {
		return err
	}
	for _, arg := range args {
		if _, err := fmt.Fprintf(writer, "$%d\r\n%s\r\n", len(arg), arg); err != nil {
			return err
		}
	}
	return nil
}

func readReply(reader *bufio.Reader) (interface{}, error) {
	prefix, err := reader.ReadByte()
	if err != nil {
		return nil, err
	}

	line, err := reader.ReadString('\n')
	if err != nil {
		return nil, err
	}
	line = strings.TrimSuffix(line, "\n")
	line = strings.TrimSuffix(line, "\r")

	switch prefix {
	case '+':
		return line, nil
	case '-':
		return nil, fmt.Errorf("dragonfly error: %s", line)
	case ':':
		value, convErr := strconv.Atoi(line)
		if convErr != nil {
			return nil, convErr
		}
		return value, nil
	case '$':
		length, convErr := strconv.Atoi(line)
		if convErr != nil {
			return nil, convErr
		}
		if length < 0 {
			return nil, nil
		}
		data := make([]byte, length+2)
		if _, err := reader.Read(data); err != nil {
			return nil, err
		}
		return string(data[:length]), nil
	default:
		return nil, fmt.Errorf("unsupported RESP prefix: %q", prefix)
	}
}
