// Copyright (c) 2021 Silvano DAL ZILIO
//
// MIT License

package rudd

// Operator describe the potential (binary) operations available on an Apply.
// Only the first four operators (from OPand to OPnand) can be used in AppEx.
type Operator int

const (
	OPand Operator = iota
	OPxor
	OPor
	OPnand
	OPnor
	OPimp
	OPbiimp
	OPdiff
	OPless
	OPinvimp
	// opnot, for negation, is the only unary operation. It should not be used
	// in Apply
	opnot
)

var opnames = [12]string{
	OPand:    "and",
	OPxor:    "xor",
	OPor:     "or",
	OPnand:   "nand",
	OPnor:    "nor",
	OPimp:    "imp",
	OPbiimp:  "biimp",
	OPdiff:   "diff",
	OPless:   "less",
	OPinvimp: "invimp",
	opnot:    "not",
}

func (op Operator) String() string {
	return opnames[op]
}

var opres = [12][2][2]int{
	//                      00    01               10    11
	OPand:    {0: [2]int{0: 0, 1: 0}, 1: [2]int{0: 0, 1: 1}}, // 0001
	OPxor:    {0: [2]int{0: 0, 1: 1}, 1: [2]int{0: 1, 1: 0}}, // 0110
	OPor:     {0: [2]int{0: 0, 1: 1}, 1: [2]int{0: 1, 1: 1}}, // 0111
	OPnand:   {0: [2]int{0: 1, 1: 1}, 1: [2]int{0: 1, 1: 0}}, // 1110
	OPnor:    {0: [2]int{0: 1, 1: 0}, 1: [2]int{0: 0, 1: 0}}, // 1000
	OPimp:    {0: [2]int{0: 1, 1: 1}, 1: [2]int{0: 0, 1: 1}}, // 1101
	OPbiimp:  {0: [2]int{0: 1, 1: 0}, 1: [2]int{0: 0, 1: 1}}, // 1001
	OPdiff:   {0: [2]int{0: 0, 1: 0}, 1: [2]int{0: 1, 1: 0}}, // 0010
	OPless:   {0: [2]int{0: 0, 1: 1}, 1: [2]int{0: 0, 1: 0}}, // 0100
	OPinvimp: {0: [2]int{0: 1, 1: 0}, 1: [2]int{0: 1, 1: 1}}, // 1011
}
