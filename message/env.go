package message

import (
	env "github.com/caarlos0/env/v10"
	"github.com/joho/godotenv"
)

func EnvLoad(v any) (err error) {
	if err = godotenv.Load(); err != nil {
		return
	}

	return env.ParseWithOptions(v, env.Options{Prefix: "MSG_"})
}
