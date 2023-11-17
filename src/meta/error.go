package meta

import (
	"fmt"
	"strings"
)

// 24~31bits: category   0~23bits：errorCode
const CategoryBitOnErrorCode = 24

type ErrorCode uint32

const (
	// Unsupported type error
	ErrUnsupportedType ErrorCode = 1 + iota
	// Repeated job
	ErrRepeatedJob
	// exceed stack limit error
	ErrStackOverflow
	// not found error
	ErrNotFound
	// invalid parameter error
	ErrInvalidParam
	// invalid job State
	ErrInvalidJobState
)

var errsMap = map[ErrorCode]string{
	ErrUnsupportedType: "unsupported type",
	ErrRepeatedJob:     "repeated job",
	ErrStackOverflow:   "exceed depth limit",
	ErrNotFound:        "not found",
	ErrInvalidParam:    "invalid parameter",
	ErrInvalidJobState: "invalid job state",
}

func NewErrorCode(behavior ErrorCode, category Category) ErrorCode {
	return behavior | (ErrorCode(category) << CategoryBitOnErrorCode)
}

func (ec ErrorCode) Category() Category {
	return Category(ec >> CategoryBitOnErrorCode)
}

func (ec ErrorCode) String() string {
	if m, ok := errsMap[ec.Behavior()]; ok {
		return m
	} else {
		return fmt.Sprintf("error code %d", ec)
	}
}

func (ec ErrorCode) Error() string {
	return ec.String()
}

func (ec ErrorCode) Behavior() ErrorCode {
	return ErrorCode(ec & 0x00ffffff)
}

type Error struct {
	Code ErrorCode
	Msg  string
	Err  error
}

func NewError(code ErrorCode, msg string, err error) Error {
	return Error{
		Code: code,
		Msg:  msg,
		Err:  err,
	}
}

func (err Error) Message() string {
	output := []string{err.Msg}
	if err.Err != nil {
		output = append(output, err.Err.Error())
	}
	return strings.Join(output, "\n")
}

func (err Error) Error() string {
	return fmt.Sprintf("[%s] %s: %s", err.Code.Category(), err.Code.Behavior(), err.Message())
}

func (err Error) Unwrap() error {
	return err.Err
}
