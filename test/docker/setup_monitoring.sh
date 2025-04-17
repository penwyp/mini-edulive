#!/bin/bash

GRAFANA_URL="http://127.0.0.1:8450"

# 检查依赖工具
command -v docker-compose >/dev/null 2>&1 || { echo "需要安装 docker-compose"; exit 1; }
command -v curl >/dev/null 2>&1 || { echo "需要安装 curl"; exit 1; }

# 调用 Grafana 配置脚本
echo "设置 Grafana 配置..."
./test/docker/setup_grafana.sh || { echo "Grafana 配置失败"; exit 1; }

# 等待服务启动并验证
echo "等待 Grafana 启动并加载配置..."
sleep 10  # 增加等待时间，确保 Grafana 完全启动并加载配置

# 重试健康检查，最多3次，每次间隔3秒
retries=3
retry_delay=3
attempt=1
while [ $attempt -le $retries ]; do
    echo "尝试健康检查 (第 $attempt 次)..."
    if curl -s "$GRAFANA_URL/api/health" >/dev/null; then
        echo "Grafana 健康检查成功"
        break
    else
        echo "Grafana 健康检查失败"
        if [ $attempt -eq $retries ]; then
            echo "达到最大重试次数，Grafana 未启动"
            exit 1
        fi
        sleep $retry_delay
        ((attempt++))
    fi
done

# 检查数据源和仪表板
curl -s "$GRAFANA_URL/api/datasources" -u admin:admin123 | grep -q "mini-edulive" && echo "数据源已加载" || echo "数据源加载失败"
curl -s "$GRAFANA_URL/api/dashboards/uid/mini-edulive-monitoring" -u admin:admin123 | grep -q "mini-edulive-monitoring" && echo "Dashboard 已加载" || echo "Dashboard 加载失败"

echo "监控服务初始化完成，请访问 $GRAFANA_URL 查看 Dashboard，测试帐号密码为 admin/admin123"