[![GoDoc](https://godoc.org/github.com/jwangsadinata/go-multimap?status.svg)](https://godoc.org/github.com/jwangsadinata/go-multimap) [![Build Status](https://travis-ci.org/jwangsadinata/go-multimap.svg)](https://travis-ci.org/jwangsadinata/go-multimap) [![Go Report Card](https://goreportcard.com/badge/github.com/jwangsadinata/go-multimap)](https://goreportcard.com/report/github.com/jwangsadinata/go-multimap) [![Coverage Status](https://coveralls.io/repos/github/jwangsadinata/go-multimap/badge.svg?branch=master&service=github)](https://coveralls.io/github/jwangsadinata/go-multimap?branch=master&service=github) [![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://github.com/jwangsadinata/go-multimap/blob/master/LICENSE)

# Go-Multimap

This is the missing `multimap` collection for the [Go](https://www.golang.org/project/) language (also a simple practice in creating a proper library/package).

A multimap (sometimes also multihash or multidict) is a generalization of a map 
or associative array abstract data type in which more than one value may be 
associated with and returned for a given key. 

Some use cases and examples for this data type includes:
- The index of a book may report any number of references for a given index term, 
and thus may be coded as a multimap from index terms to any number of reference locations or pages.
- Address location, such as ZIP code, that maps to any number of people living in that area.

There are two different multimap implementations, `slicemultimap` and `setmultimap`, which has slices and sets 
as the map values respectively. `slicemultimap` is useful when duplicate key/value pairs is allowed and 
insertion ordering is important. On the other hand, `setmultimap` is suitable when duplicates of key/value 
pairs are not allowed.

This package was heavily inspired by the Google Guava interface of MultiMap and 
written in the style of the [container](https://golang.org/pkg/container/) package.


References: 
[Wikipedia](https://en.wikipedia.org/wiki/Multimap), 
[Guava](https://google.github.io/guava/releases/19.0/api/docs/com/google/common/collect/Multimap.html)

## Installation ##

Install the package via the following:

    go get -u github.com/jwangsadinata/go-multimap

## Usage ##

The go-multimap package can be used similarly to the following:
```go
// example/example.go
package main

import (
	"fmt"

	"github.com/jwangsadinata/go-multimap/slicemultimap"
)

func main() {
	usPresidents := []struct {
		firstName  string
		middleName string
		lastName   string
		termStart  int
		termEnd    int
	}{
		{"George", "", "Washington", 1789, 1797},
		{"John", "", "Adams", 1797, 1801},
		{"Thomas", "", "Jefferson", 1801, 1809},
		{"James", "", "Madison", 1809, 1817},
		{"James", "", "Monroe", 1817, 1825},
		{"John", "Quincy", "Adams", 1825, 1829},
		{"John", "", "Tyler", 1841, 1845},
		{"James", "", "Polk", 1845, 1849},
		{"Grover", "", "Cleveland", 1885, 1889},
		{"Benjamin", "", "Harrison", 1889, 1893},
		{"Grover", "", "Cleveland", 1893, 1897},
		{"George", "Herbert Walker", "Bush", 1989, 1993},
		{"George", "Walker", "Bush", 2001, 2009},
		{"Barack", "Hussein", "Obama", 2009, 2017},
	}

	m := slicemultimap.New()

	for _, president := range usPresidents {
		m.Put(president.firstName, president.lastName)
	}

	for _, firstName := range m.KeySet() {
		lastNames, _ := m.Get(firstName)
		fmt.Printf("%v: %v\n", firstName, lastNames)
	}
}
```

Example output:
```sh
$ go run example.go
George: [Washington Bush Bush]
John: [Adams Adams Tyler]
Thomas: [Jefferson]
James: [Madison Monroe Polk]
Grover: [Cleveland Cleveland]
Benjamin: [Harrison]
Barack: [Obama]
```

## Benchmarks ##
To see the benchmark, run the following on each of the sub-packages:

`go test -run=NO_TEST -bench . -benchmem  -benchtime 1s ./...`

<p align="center"><img width="569" src="https://user-images.githubusercontent.com/13155377/38164450-3e916478-352f-11e8-8a4b-be10f60df4f9.png" /></p>
<p align="center"><img width="568" src="https://user-images.githubusercontent.com/13155377/38164452-435d48aa-352f-11e8-8a70-7a54f41b1610.png" /></p>


Please see [the GoDoc API page](http://godoc.org/github.com/jwangsadinata/go-multimap) for a
full API listing. For more examples, please consult `example_test.go` file located in each subpackages.
