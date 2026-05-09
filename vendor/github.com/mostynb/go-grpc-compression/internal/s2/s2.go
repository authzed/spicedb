// Copyright 2022 Mostyn Bramley-Moore.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package s2 is an experimental wrapper for using
// github.com/klauspost/compress/s2 stream compression with gRPC.
package s2

import (
	"io"
	"io/ioutil"
	"sync"

	"github.com/klauspost/compress/s2"
	"google.golang.org/grpc/encoding"
)

const Name = "s2"

type compressor struct {
	poolCompressor   sync.Pool
	poolDecompressor sync.Pool
}

type writer struct {
	*s2.Writer
	pool *sync.Pool
}

type reader struct {
	*s2.Reader
	pool *sync.Pool
}

func PretendInit(clobbering bool) {
	if !clobbering && encoding.GetCompressor(Name) != nil {
		return
	}

	c := &compressor{}
	c.poolCompressor.New = func() interface{} {
		w := s2.NewWriter(ioutil.Discard, s2.WriterConcurrency(1))
		return &writer{Writer: w, pool: &c.poolCompressor}
	}
	encoding.RegisterCompressor(c)
}

func (c *compressor) Compress(w io.Writer) (io.WriteCloser, error) {
	s := c.poolCompressor.Get().(*writer)
	s.Writer.Reset(w)
	return s, nil
}

func (c *compressor) Decompress(r io.Reader) (io.Reader, error) {
	s, inPool := c.poolDecompressor.Get().(*reader)
	if !inPool {
		newR := s2.NewReader(r)
		return &reader{Reader: newR, pool: &c.poolDecompressor}, nil
	}
	s.Reset(r)
	return s, nil
}

func (c *compressor) Name() string {
	return Name
}

func (s *writer) Close() error {
	err := s.Writer.Close()
	s.pool.Put(s)
	return err
}

func (s *reader) Read(p []byte) (n int, err error) {
	n, err = s.Reader.Read(p)
	if err == io.EOF {
		s.pool.Put(s)
	}
	return n, err
}
