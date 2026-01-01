package overseer

import (
	"context"
	"errors"
)

type Overseer struct {
}

func (ovr *Overseer) Register(ctx context.Context, addr string) error {
	return errors.New("not implemented")
}
