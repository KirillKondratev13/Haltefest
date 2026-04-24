package config

import "os"

type Config struct {
	ServerAddr        string
	AssetsDir         string
	DBConnStr         string
	FilerURL          string
	KafkaBrokers      string
	DragonflyAddr     string
	DragonflyPassword string
}

func NewConfig() Config {
	return Config{
		ServerAddr:        getEnv("SERVER_ADDR", ":8081"),
		AssetsDir:         getEnv("ASSETS_DIR", "./web/public/assets"),
		DBConnStr:         getEnv("DB_CONN_STR", "postgres://myappuser:mypassword@localhost:5432/myapp"),
		FilerURL:          getEnv("FILER_URL", "http://localhost:8888"),
		KafkaBrokers:      getEnv("KAFKA_BROKERS", "localhost:9092"),
		DragonflyAddr:     getEnv("DRAGONFLY_ADDR", ""),
		DragonflyPassword: getEnv("DRAGONFLY_PASSWORD", ""),
	}
}

func getEnv(key, fallback string) string {
	if value, ok := os.LookupEnv(key); ok {
		return value
	}
	return fallback
}
