package storage

import (
	"fmt"
	"io"
	"net/http"
	"time"
)

type SeaweedClient struct {
	FilerURL string
	client   *http.Client
}

func NewSeaweedClient(filerURL string) *SeaweedClient {
	return &SeaweedClient{
		FilerURL: filerURL,
		client: &http.Client{
			Timeout: 30 * time.Second,
		},
	}
}

func (s *SeaweedClient) Download(path string) ([]byte, error) {
	url := fmt.Sprintf("%s%s", s.FilerURL, path)
	resp, err := s.client.Get(url)
	if err != nil {
		return nil, fmt.Errorf("failed to download from SeaweedFS: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status code from SeaweedFS: %d", resp.StatusCode)
	}

	return io.ReadAll(resp.Body)
}
