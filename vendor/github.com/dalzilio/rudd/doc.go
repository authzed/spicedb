// Copyright (c) 2021 Silvano DAL ZILIO
//
// MIT License

/*
Package rudd defines a concrete type for Binary Decision Diagrams (BDD), a data
structure used to efficiently represent Boolean functions over a fixed set of
variables or, equivalently, sets of Boolean vectors with a fixed size.

Basics

Each BDD has a fixed number of variables, Varnum, declared when it is
initialized (using the method New) and each variable is represented by an
(integer) index in the interval [0..Varnum), called a level. Our library support
the creation of multiple BDD with possibly different number of variables.

Most operations over BDD return a Node; that is a pointer to a "vertex" in the
BDD that includes a variable level, and the address of the low and high branch
for this node. We use integer to represent the address of Nodes, with the
convention that 1 (respectively 0) is the address of the constant function True
(respectively False).

Use of build tags

For the most part, data structures and algorithms implemented in this library
are a direct adaptation of those found in the  C-library BuDDy, developed by
Jorn Lind-Nielsen; we even implemented the same examples than in the BuDDy
distribution for benchmarks and regression testing. We provide two possible
implementations for BDD that can be selected using build tags.

Our default implementation (without build tag) use a standard Go runtime hashmap
to encode a "unicity table".

When building your executable with the build tag `buddy`, the API will switch to
an implementation that is very close to the one of the BuDDy library; based on a
specialized data-structure that mix a dynamic array with a hash table.

To get access to better statistics about caches and garbage collection, as well
as to unlock logging of some operations, you can also compile your executable
with the build tag `debug`.

Automatic memory management

The library is written in pure Go, without the need for CGo or any other
dependencies. Like with MuDDy, a ML interface to BuDDy, we piggyback on the
garbage collection mechanism offered by our host language (in our case Go). We
take care of BDD resizing and memory management directly in the library, but
"external" references to BDD nodes made by user code are automatically managed
by the Go runtime. Unlike MuDDy, we do not provide an interface, but a genuine
reimplementation of BDD in Go. As a consequence, we do not suffer from FFI
overheads when calling from Go into C.
*/
package rudd
