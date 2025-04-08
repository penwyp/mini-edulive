#!/bin/bash

# 设置输出文件
output_file="data/project_structure.txt"

# 清空或创建输出文件
> "$output_file"

# 打印项目目录结构
echo "Project Structure:" >> "$output_file"
echo "=================" >> "$output_file"
tree -I 'vendor|node_modules|googleapis|grpc-proto|.git' >> "$output_file"
echo -e "\n\n" >> "$output_file"

# 遍历所有目标文件类型并追加内容
echo "Source Code and Configuration:" >> "$output_file"
echo "=============================" >> "$output_file"

# 定义要包含的文件类型
file_types=("*.go" "Makefile" "*.proto" "*.yaml" "*.yml" "go.mod")

mkdir -p data

for type in "${file_types[@]}"; do
    find . -name "$type" -not -path "*/vendor/*" -not -path "*/proto/lib/googleapis/*"  -not -path "*/proto/lib/grpc-proto/*" -not -path "*/node_modules/*" | while read -r file; do
        echo -e "\n\n----------------\nFile: $file" >> "$output_file"
        echo "----------------" >> "$output_file"
        cat "$file" >> "$output_file"
    done
done

echo "Done! Output saved to $output_file"