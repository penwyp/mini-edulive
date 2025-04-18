package util

import "strconv"

// Max 计算最大值
func Max(weights []int) int {
	if len(weights) == 0 {
		return 0
	}
	m := weights[0]
	for _, w := range weights[1:] {
		if w > m {
			m = w
		}
	}
	return m
}

// GCD 计算最大公约数
func GCD(weights []int) int {
	if len(weights) == 0 {
		return 0
	}
	result := weights[0]
	for i := 1; i < len(weights); i++ {
		result = GCDTwo(result, weights[i])
	}
	return result
}

// GCDTwo 计算两个数的最大公约数
func GCDTwo(a, b int) int {
	for b != 0 {
		a, b = b, a%b
	}
	return a
}

// FormatUint64ToString format uint64 to string
func FormatUint64ToString(num uint64) string {
	return strconv.FormatUint(num, 10)
}

// FormatInt64ToString format uint64 to string
func FormatInt64ToString(num int64) string {
	return strconv.FormatInt(num, 10)
}
