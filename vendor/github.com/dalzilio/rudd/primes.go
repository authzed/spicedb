// Copyright (c) 2021 Silvano DAL ZILIO
//
// MIT License

package rudd

import "math/big"

// functions for Prime number calculations

func hasFactor(src int, n int) bool {
	if (src != n) && (src%n == 0) {
		return true
	}
	return false
}

func hasEasyFactors(src int) bool {
	return hasFactor(src, 3) || hasFactor(src, 5) || hasFactor(src, 7) || hasFactor(src, 11) || hasFactor(src, 13)
}

func primeGte(src int) int {
	if src%2 == 0 {
		src++
	}

	for {
		if hasEasyFactors(src) {
			src = src + 2
			continue
		}
		// ProbablyPrime is 100% accurate for inputs less than 2⁶⁴.
		if big.NewInt(int64(src)).ProbablyPrime(0) {
			return src
		}
		src = src + 2
	}
}

func primeLte(src int) int {
	if src == 0 {
		return 1
	}

	if src%2 == 0 {
		src--
	}

	for {
		if hasEasyFactors(src) {
			src = src - 2
			continue
		}
		// ProbablyPrime is 100% accurate for inputs less than 2⁶⁴.
		if big.NewInt(int64(src)).ProbablyPrime(0) {
			return src
		}
		src = src - 2
	}
}
