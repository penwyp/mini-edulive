package util

import (
	"math/rand"
	"time"
)

var colors = []string{"red", "green", "blue", "yellow", "cyan", "magenta"}

func init() {
	rand.Seed(time.Now().UnixNano())
}

func GetDefaultColorOrRandom(color string) string {
	if color != "" {
		return color
	}
	return colors[rand.Intn(len(colors))]
}

func GetRandomColor() string {
	return colors[rand.Intn(len(colors))]
}
