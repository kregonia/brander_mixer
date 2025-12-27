package bootstrap

import (
	"context"

	logger "github.com/kregonia/brander_mixer/log"
)

func Bootstrap(ctx context.Context) {
	err := logger.Init(ctx, logger.Config{})
	if err != nil {
		panic(err)
	}
	go func() {
		<-ctx.Done()
		logger.CloseAll()
	}()
}
