package notimmediatelyclosed

import (
	"fmt"
)

type SomeIterator struct{}

func (si *SomeIterator) Close() {
}

func (si *SomeIterator) Err() error {
	return nil
}

func (si *SomeIterator) Next() any {
	return nil
}

func (si *SomeIterator) Cursor() (int, error) {
	return 42, nil
}

func MissingNonDeferClose() {
	si := CreateIterator() // want "variable MissingNonDeferClose.si is missing required non-defer-ed"
	defer si.Close()

	for v := si.Next(); v != nil; v = si.Next() {
		fmt.Println(v)
	}

	fmt.Println("another op")
}

func CloseInWrongPlace() {
	si := CreateIterator()
	defer si.Close()

	for v := si.Next(); v != nil; v = si.Next() { // want "expected variable CloseInWrongPlace.si to have a call to Close after here"
		fmt.Println(v)
	}

	fmt.Println("another op")
	si.Close()
}

func CloseRightAfterUse() {
	si := CreateIterator()
	defer si.Close()

	for v := si.Next(); v != nil; v = si.Next() {
		fmt.Println(v)
	}
	si.Close()

	fmt.Println("another op")
}

func SecondCloseRightAfterUse() {
	si := CreateIterator()
	defer si.Close()

	for v := si.Next(); v != nil; v = si.Next() {
		fmt.Println(v)
	}
	si.Close()
}

func SentToAnotherFunctionAndNotClosed() {
	si := CreateIterator() // want "variable SentToAnotherFunctionAndNotClosed.si is missing required non-defer-ed Close"
	defer si.Close()

	AnotherFunction(si)
}

func SentToAnotherFunctionAndClosed() {
	si := CreateIterator()
	defer si.Close()

	AnotherFunction(si)
	si.Close()
}

func SentToThirdFunctionAndClosed() {
	si := CreateIterator()
	defer si.Close()

	err := ThirdFunction(si)
	si.Close()
	if err != nil {
		panic("oh no!")
	}
}

func SentToThirdFunctionAndClosedAfter() {
	si := CreateIterator()
	defer si.Close()

	err := ThirdFunction(si) // want "expected variable SentToThirdFunctionAndClosedAfter.si to have a call to Close after here"
	if err != nil {
		panic("oh no!")
	}
	si.Close()
}

func SentToFunctionUnderSwitch() {
	switch {
	case true:
		si := CreateIterator()
		defer si.Close()

		err := ThirdFunction(si)
		si.Close()
		if err != nil {
			panic("oh no!")
		}
	}
}

func AccessedAfterLoop() error {
	cur := 0
	for {
		si, err := CreateIteratorOrError()
		if err != nil {
			return nil
		}

		for tpl := si.Next(); tpl != nil; tpl = si.Next() {
			// do nothing
		}
		if si.Err() != nil {
			return si.Err()
		}

		if cur == 0 {
			si.Close()
			continue
		}

		cur, err = si.Cursor()
		si.Close()

		if err != nil {
			return err
		}
		fmt.Println(cur)
	}
}

func AccessedDirectly() error {
	var err error
	si := CreateIterator()
	_, err = si.Cursor()
	si.Close()
	return err
}

func ThirdFunction(si *SomeIterator) error {
	return nil
}

func AnotherFunction(si *SomeIterator) {
	si.Close()
}

func CreateIterator() *SomeIterator {
	return &SomeIterator{}
}

func CreateIteratorOrError() (*SomeIterator, error) {
	return &SomeIterator{}, nil
}
