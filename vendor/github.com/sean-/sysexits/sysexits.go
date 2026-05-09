// Copyright (c) 2017 Sean Chittenden.
// Copyright (c) 1987, 1993
//	The Regents of the University of California.  All rights reserved.
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions
// are met:
// 1. Redistributions of source code must retain the above copyright
//    notice, this list of conditions and the following disclaimer.
// 2. Redistributions in binary form must reproduce the above copyright
//    notice, this list of conditions and the following disclaimer in the
//    documentation and/or other materials provided with the distribution.
// 3. Neither the name of the University nor the names of its contributors
//    may be used to endorse or promote products derived from this software
//    without specific prior written permission.
//
// THIS SOFTWARE IS PROVIDED BY THE REGENTS AND CONTRIBUTORS ``AS IS'' AND
// ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
// IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
// ARE DISCLAIMED.  IN NO EVENT SHALL THE REGENTS OR CONTRIBUTORS BE LIABLE
// FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
// DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS
// OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
// HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
// LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY
// OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
// SUCH DAMAGE.
//
//	@(#)sysexits.h	8.1 (Berkeley) 6/2/93
//
// $FreeBSD: head/include/sysexits.h 203964 2010-02-16 19:39:50Z imp $

package sysexits

import "fmt"

type ExitCode int

const (
	// OK indicates successful termination
	OK = 0

	// Base is the minimum value for an error code
	Base = 64 // base value for error messages

	// Usage indicates the command was used incorrectly, e.g., with the wrong
	// number of arguments, a bad flag, a bad syntax in a parameter, or whatever.
	Usage = 64

	// DataErr indicates the input data was incorrect in some way.  This should
	// only be used for user's data & not system files.
	DataErr = 65

	// NoInput indicates an input file (not a system file) did not exist or was
	// not readable.  This could also include errors like "No message" to a mailer
	// (if it cared to catch it).
	NoInput = 66

	// NoUser indicates the user specified did not exist.  This might be used for
	// mail addresses or remote logins.
	NoUser = 67

	// NoHost indicates the host specified did not exist.  This is used in mail
	// addresses or network requests.
	NoHost = 68

	// Unavailable indicates a service is unavailable.  This can occur if a
	// support program or file does not exist.  This can also be used as a
	// catchall message when something you wanted to do doesn't work, but you
	// don't know why.
	Unavailable = 69

	// Software indicates an internal software error has been detected.  This
	// should be limited to non-operating system related errors as possible.
	Software = 70

	// OSErr indicates an operating system error has been detected.  This is
	// intended to be used for such things as "cannot fork", "cannot create pipe",
	// or the like.  It includes things like getuid returning a user that does not
	// exist in the passwd file.
	OSErr = 71

	// OSFile indicates some system file (e.g., /etc/passwd, /etc/utmp, etc.) does
	// not exist, cannot be opened, or has some sort of error (e.g., syntax
	// error).
	OSFile = 72

	// CantCreate indcates a (user specified) output file cannot be
	// 	created.
	CantCreate = 73

	// IOErr indicates an error occurred while doing I/O on some file.
	IOErr = 74

	// TempFail indicates temporary failure, indicating something that is not
	// really an error.  In sendmail, this means that a mailer (e.g.) could not
	// create a connection, and the request should be reattempted later.
	TempFail = 75

	// Protocol indicates the remote system returned something that was "not
	// possible" during a protocol exchange.
	Protocol = 76

	// NoPerm indicates you did not have sufficient permission to perform the
	// operation.  This is not intended for file system problems, which should use
	// NoInput or CantCreate, but rather for higher level permissions.
	NoPerm = 77

	// Config indicates a configuration error.
	Config = 78

	MAX = 78 // maximum listed value
)

func (ec ExitCode) String() string {
	switch ec {
	case OK:
		return "successful termination"
	case Usage:
		return "command line usage error"
	case DataErr:
		return "data format error"
	case NoInput:
		return "cannot open input"
	case NoUser:
		return "addressee unknown"
	case NoHost:
		return "host name unknown"
	case Unavailable:
		return "service unavailable"
	case Software:
		return "internal software error"
	case OSErr:
		return "system error (e.g., can't fork)"
	case OSFile:
		return "critical OS file missing"
	case CantCreate:
		return "can't create (user) output file"
	case IOErr:
		return "input/output error"
	case TempFail:
		return "temp failure; user is invited to retry"
	case Protocol:
		return "remote error in protocol"
	case NoPerm:
		return "permission denied"
	case Config:
		return "configuration error"
	default:
		return fmt.Sprintf("unknown exit code: %d", ec)
	}
}
