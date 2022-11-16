package code

import (
	"fmt"
)

type Error interface {
	error
	Mcode() string
	Message() string
}

type errorImpl struct {
	message string
	mcode   string
}

func (err *errorImpl) Error() string {
	return fmt.Sprintf("%s,%s", err.mcode, err.message)
}

func (err *errorImpl) Message() string {

	return err.message
}

func (err *errorImpl) Mcode() string {
	return err.mcode
}

func NewMcode(mcode string, msg string) Error {
	return &errorImpl{
		mcode:   mcode,
		message: msg,
	}
}

func NewMcodef(mcode string, format string, args ...interface{}) Error {
	return &errorImpl{
		mcode:   mcode,
		message: fmt.Sprintf(format, args...),
	}
}
