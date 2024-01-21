package pkg

import "errors"

func wrapError(err error, msg string) error {
	return errors.New(msg + ", cause " + err.Error())
}
