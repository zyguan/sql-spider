package util

import (
	"os"
	"path"
	"sync"

	"github.com/joho/godotenv"
)

var loadDotEnvOnce sync.Once

func LoadDotEnvOnce(cbs ...func()) {
	loadDotEnvOnce.Do(func() {
		LoadDotEnv(cbs...)
	})
}

func LoadDotEnv(cbs ...func()) bool {
	defer func() {
		for _, cb := range cbs {
			cb()
		}
	}()
	d, err := os.Getwd()
	if err != nil {
		return false
	}
	for len(d) > 0 {
		if err := godotenv.Load(path.Join(d, ".env")); err == nil {
			return true
		}
		lastLen := len(d)
		d = path.Dir(d)
		if len(d) == lastLen {
			break
		}
	}
	return false
}

func GetEnv(name string, optDefault ...string) string {
	if val := os.Getenv(name); len(val) > 0 {
		return val
	}
	if len(optDefault) > 0 {
		return optDefault[0]
	}
	return ""
}
