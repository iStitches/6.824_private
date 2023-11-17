package mr

import "6.5840/meta"

func wrapError(code meta.ErrorCode, msg string, err error) error {
	return meta.NewError(meta.NewErrorCode(code, meta.MR_DIR), msg, err)
}

func unwrapError(msg string, err error) error {
	if v, ok := err.(meta.Error); ok {
		return wrapError(v.Code, msg, err)
	} else {
		return wrapError(0, msg, err)
	}
}
