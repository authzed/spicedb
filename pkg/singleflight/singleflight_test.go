package singleflight

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"
)

func TestCancelation(t *testing.T) {
	g := Group[string, string]{}

	first, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	second, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	firstChan, _ := g.DoChan(first, "fun", func(ctx context.Context) (string, error) {
		t := time.After(2 * time.Second)
		select {
		case <-t:
			return "first value", nil
		case <-ctx.Done():
			return "", errors.New("first context canceled")
		}
	})
	secondChan, _ := g.DoChan(second, "fun", func(ctx context.Context) (string, error) {
		t := time.After(2 * time.Second)
		select {
		case <-t:
			return "second value", nil
		case <-ctx.Done():
			return "", errors.New("second context canceled")
		}
	})

	for i := 0; i < 2; i++ {
		select {
		case firstResult := <-firstChan:
			fmt.Printf("first result: %s %s\n", firstResult.Val, firstResult.Err)
		case secondResult := <-secondChan:
			fmt.Printf("second result: %s %s\n", secondResult.Val, secondResult.Err)
		}
	}
}
