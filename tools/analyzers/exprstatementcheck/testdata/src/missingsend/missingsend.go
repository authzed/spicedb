package _missingsend

import (
	"errors"
)

func SomeFunction() {
	Err(errors.New("foo")) // want "missing Send or Msg for zerolog log statement"
}

func AnotherFunction() {
	Err(errors.New("foo")).Send()
}
