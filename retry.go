package kapacitor

import "github.com/pkg/errors"

// temporary enables an error to indicate whether
// it is temporary and safe for retrying.
type temporary interface {
	Temporary() bool
}

// doWhileTemporary runs a function while its error not nil and is temporary.
func DoWhileTemporary(f func() error) (err error) {
	temp := true
	for temp {
		err = f()
		if err == nil {
			return
		}
		temp = isTemporary(errors.Cause(err))
	}
	return
}

func isTemporary(err error) bool {
	if t, ok := err.(temporary); ok {
		return t.Temporary()
	}
	return false
}
