package config

type Config struct {
	ServerAddr string
	AssetsDir string
	DBConnStr  string
	SeaweedFSMasterURL string // "http://localhost:9333"
	SeaweedFSVolumeURL string
}

func NewConfig() Config {
	return Config{
		ServerAddr: ":8081",
		AssetsDir: "./web/public/assets",
		DBConnStr:  "postgres://myappuser:mypassword@localhost:5432/myapp",
		SeaweedFSMasterURL: "http://localhost:9333",
		SeaweedFSVolumeURL: "http://localhost:8080",
	}
}

