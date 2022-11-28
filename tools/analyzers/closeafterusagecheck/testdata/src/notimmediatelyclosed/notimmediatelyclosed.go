package notimmediatelyclosed

import (
	"fmt"
)

type SomeIterator struct{}

func (si *SomeIterator) Close() {
}

func (si *SomeIterator) Next() any {
	return nil
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

func ThirdFunction(si *SomeIterator) error {
	return nil
}

func AnotherFunction(si *SomeIterator) {
	si.Close()
}

func CreateIterator() *SomeIterator {
	return &SomeIterator{}
}
