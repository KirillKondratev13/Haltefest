package config

import (
	"os"
)

type Config struct {
	KafkaBrokers string
	FilerURL     string
	RedisURL     string
	DBConnStr    string
	WorkerCount  int
}

func LoadConfig() *Config {
	return &Config{
		KafkaBrokers: getEnv("KAFKA_BROKERS", "localhost:9092"),
		FilerURL:     getEnv("FILER_URL", "http://localhost:8888"),
		RedisURL:     getEnv("REDIS_URL", "redis://localhost:6379"),
		DBConnStr:    getEnv("DB_CONN_STR", "postgres://myappuser:mypassword@localhost:5432/myapp"),
		WorkerCount:  5, // Default to 5 workers as requested parallel processing
	}
}

func getEnv(key, fallback string) string {
	if value, ok := os.LookupEnv(key); ok {
		return value
	}
	return fallback
}
