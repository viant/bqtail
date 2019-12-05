package base

import (
	"math/rand"
	"time"
)

//GenerateInt generates a random int
func GenerateInt(min, max int) int {
	rnd := rand.New(rand.NewSource(time.Now().UnixNano()))
	return min + int(rnd.Int63())%(max-min)
}
