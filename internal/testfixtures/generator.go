package testfixtures

import "math/rand"

const (
	FirstLetters      = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890_"
	SubsequentLetters = FirstLetters + "/_|-"
)

func RandomObjectID(length uint8) string {
	b := make([]byte, length)
	for i := range b {
		sourceLetters := SubsequentLetters
		if i == 0 {
			sourceLetters = FirstLetters
		}
		b[i] = sourceLetters[rand.Intn(len(sourceLetters))]
	}
	return string(b)
}
