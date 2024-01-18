package rcgo

import (
	"os"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

func setupLogger(loc *time.Location, level string) error {
	zerolog.TimestampFunc = func() time.Time {
		return time.Now().In(loc)
	}

	lvl, err := zerolog.ParseLevel(level)
	if err != nil {
		return err
	}

	zerolog.SetGlobalLevel(lvl)

	log.Logger = zerolog.New(os.Stdout).With().Timestamp().Logger()

	return nil
}
