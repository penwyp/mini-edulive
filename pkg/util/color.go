package util

import (
	"math/rand"
	"time"

	"github.com/fatih/color"
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

// GetColorFunc 根据颜色字符串返回对应的彩色打印函数
func GetColorFunc(colorStr string) func(string, ...interface{}) (n int, err error) {
	switch colorStr {
	case "red":
		return color.New(color.FgRed).Printf
	case "green":
		return color.New(color.FgGreen).Printf
	case "blue":
		return color.New(color.FgBlue).Printf
	case "yellow":
		return color.New(color.FgYellow).Printf
	case "cyan":
		return color.New(color.FgCyan).Printf
	case "magenta":
		return color.New(color.FgMagenta).Printf
	default:
		return color.New(color.FgWhite).Printf
	}
}
