package query

import "io"

// setOperationFlags is the shared wire definition for unions and intersections.
// Keep field-to-bit mappings and their encoding/decoding together here so both
// iterator types interpret the same flags. Concurrency limits remain local policy.
type setOperationFlags struct {
	independentCheckBranches bool
}

// This bit is part of the existing wire format and must retain its position.
const independentCheckBranchesFlag uint = 0

func (flags setOperationFlags) write(w io.Writer) error {
	var bits uint64
	setFlag(&bits, independentCheckBranchesFlag, flags.independentCheckBranches)
	return writeUvarint(w, bits)
}

func readSetOperationFlags(r io.ByteReader) (setOperationFlags, error) {
	bits, err := readUvarint(r)
	if err != nil {
		return setOperationFlags{}, err
	}

	return setOperationFlags{
		independentCheckBranches: hasFlag(bits, independentCheckBranchesFlag),
	}, nil
}
