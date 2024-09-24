package lenexamples

import "fmt"

func DoSomething(someSlice []string) uint64 {
	return uint64(len(someSlice))
}

func DoSomethingBad(someSlice []string) uint32 {
	v := uint32(len(someSlice)) // want "found downcast of `len` call to uint32"
	return v
}

func DoSomethingBad16(someSlice []string) uint16 {
	v := uint16(len(someSlice)) // want "found downcast of `len` call to uint16"
	return v
}

func DoSomeLoop(someSlice []string) {
	for i := 0; i < len(someSlice); i++ {
		fmt.Println(someSlice[i])
	}
}
