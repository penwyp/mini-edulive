package util

import "strings"

// ContainsCaseInsensitive 检查 source 字符串是否包含 substr（不区分大小写）
// 返回：如果包含则返回 true，否则返回 false
// 使用场景：在敏感词检查中，判断弹幕内容是否包含敏感词
func ContainsCaseInsensitive(source, substr string) bool {
	// 将源字符串和子字符串转换为小写进行比较
	return strings.Contains(strings.ToLower(source), strings.ToLower(substr))
}
