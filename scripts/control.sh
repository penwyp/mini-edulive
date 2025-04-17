#!/bin/bash

# 项目基本信息
BIN_DIR="bin"
GATEWAY_BINARY="${BIN_DIR}/mini-edulive-gateway"
DISPATCHER_BINARY="${BIN_DIR}/mini-edulive-dispatcher"
WORKER_BINARY="${BIN_DIR}/mini-edulive-worker"
CLIENT_BINARY="${BIN_DIR}/mini-edulive-client"

# PID 文件路径
GATEWAY_PID="logs/gateway.pid"
DISPATCHER_PID="logs/dispatcher.pid"
WORKER_PID="logs/worker.pid"
CLIENT_PID="logs/client.pid"

# 日志文件路径
GATEWAY_LOG="logs/edulive_gateway.log"
DISPATCHER_LOG="logs/edulive_dispatcher.log"
WORKER_LOG="logs/edulive_worker.log"
CLIENT_LOG="logs/edulive_client.log"

# 客户端配置文件（默认使用 config_client.yaml，可根据需要修改）
CLIENT_CONFIG="config/config_client.yaml"

# 确保日志目录存在
mkdir -p logs

# 显示帮助信息
usage() {
    echo "Usage: $0 {build|start|stop|restart|status} [gateway|dispatcher|worker|client|all]"
    echo "Examples:"
    echo "  $0 build all          # 编译所有组件"
    echo "  $0 start gateway      # 启动 gateway"
    echo "  $0 stop all           # 停止所有组件"
    echo "  $0 restart worker     # 重启 worker"
    echo "  $0 status all         # 查看所有组件状态"
    exit 1
}

# 检查命令和组件参数
if [ $# -lt 2 ]; then
    usage
fi

COMMAND=$1
COMPONENT=$2

# 检查组件是否有效
valid_component() {
    case "$1" in
        gateway|dispatcher|worker|client|all)
            return 0
            ;;
        *)
            echo "Invalid component: $1. Must be 'gateway', 'dispatcher', 'worker', 'client', or 'all'."
            exit 1
            ;;
    esac
}

# 检查 Makefile 是否存在
check_makefile() {
    if [ ! -f "Makefile" ]; then
        echo "Error: Makefile not found in the current directory."
        exit 1
    fi
}

# 编译指定组件
build_component() {
    local component=$1
    check_makefile
    echo "Building $component..."
    case "$component" in
        gateway)
            make build-gateway
            ;;
        dispatcher)
            make build-dispatcher
            ;;
        worker)
            make build-worker
            ;;
        client)
            make build-client
            ;;
        all)
            make build
            ;;
    esac
    if [ $? -eq 0 ]; then
        echo "$component built successfully."
    else
        echo "Failed to build $component."
        exit 1
    fi
}

# 检查进程是否运行
check_pid() {
    local pid_file=$1
    if [ -f "$pid_file" ]; then
        local pid=$(cat "$pid_file")
        if ps -p "$pid" > /dev/null 2>&1; then
            return 0 # 进程存在
        else
            rm -f "$pid_file" # 移除无效的 PID 文件
            return 1 # 进程不存在
        fi
    fi
    return 1
}

# 启动指定组件
start_component() {
    local component=$1
    local binary=""
    local pid_file=""
    local log_file=""
    local extra_args=""
    case "$component" in
        gateway)
            binary=$GATEWAY_BINARY
            pid_file=$GATEWAY_PID
            log_file=$GATEWAY_LOG
            ;;
        dispatcher)
            binary=$DISPATCHER_BINARY
            pid_file=$DISPATCHER_PID
            log_file=$DISPATCHER_LOG
            ;;
        worker)
            binary=$WORKER_BINARY
            pid_file=$WORKER_PID
            log_file=$WORKER_LOG
            ;;
        client)
            binary=$CLIENT_BINARY
            pid_file=$CLIENT_PID
            log_file=$CLIENT_LOG
            extra_args="-config $CLIENT_CONFIG"
            ;;
    esac

    # 检查二进制文件是否存在
    if [ ! -f "$binary" ]; then
        echo "Binary $binary not found. Building $component..."
        build_component "$component"
    fi

    # 检查是否已在运行
    if check_pid "$pid_file"; then
        echo "$component is already running (PID: $(cat "$pid_file"))."
        return
    fi

    # 清理旧日志文件
    echo '' > "$log_file"

    # 启动进程
    echo "Starting $component..."
    if [ -n "$extra_args" ]; then
        nohup "$binary" $extra_args > "$log_file" 2>&1 &
    else
        nohup "$binary" > "$log_file" 2>&1 &
    fi
    local pid=$!
    sleep 1 # 等待进程启动

    # 检查进程是否启动成功
    if ps -p "$pid" > /dev/null 2>&1; then
        echo "$pid" > "$pid_file"
        echo "$component started successfully (PID: $pid)."
    else
        echo "Failed to start $component. Check logs at $log_file."
        return 1
    fi
    return 0
}

# 停止指定组件
stop_component() {
    local component=$1
    local pid_file=""
    case "$component" in
        gateway)
            pid_file=$GATEWAY_PID
            ;;
        dispatcher)
            pid_file=$DISPATCHER_PID
            ;;
        worker)
            pid_file=$WORKER_PID
            ;;
        client)
            pid_file=$CLIENT_PID
            ;;
    esac

    if [ ! -f "$pid_file" ]; then
        echo "$component is not running (no PID file found)."
        return 0
    fi

    local pid=$(cat "$pid_file")
    if ps -p "$pid" > /dev/null 2>&1; then
        echo "Stopping $component (PID: $pid)..."
        kill -9 "$pid"
        local timeout=10
        while [ $timeout -gt 0 ]; do
            if ! ps -p "$pid" > /dev/null 2>&1; then
                rm -f "$pid_file"
                echo "$component stopped successfully."
                return 0
            fi
            sleep 1
            timeout=$((timeout - 1))
        done
        echo "Failed to stop $component gracefully. Forcing termination..."
        kill -9 "$pid" 2>/dev/null
        rm -f "$pid_file"
        echo "$component forcibly stopped."
    else
        echo "$component is not running."
        rm -f "$pid_file"
    fi
    return 0
}

# 停止所有已启动的组件（用于错误处理）
stop_all_started() {
    echo "Cleaning up: Stopping all started components..."
    for comp in client worker dispatcher gateway; do
        stop_component "$comp"
    done
}

# 重启指定组件
restart_component() {
    local component=$1
    stop_component "$component"
    sleep 1
    start_component "$component" || {
        echo "Failed to restart $component. Cleaning up..."
        stop_all_started
        exit 1
    }
}

# 查看指定组件状态
status_component() {
    local component=$1
    local pid_file=""
    local log_file=""
    case "$component" in
        gateway)
            pid_file=$GATEWAY_PID
            log_file=$GATEWAY_LOG
            ;;
        dispatcher)
            pid_file=$DISPATCHER_PID
            log_file=$DISPATCHER_LOG
            ;;
        worker)
            pid_file=$WORKER_PID
            log_file=$WORKER_LOG
            ;;
        client)
            pid_file=$CLIENT_PID
            log_file=$CLIENT_LOG
            ;;
    esac

    if check_pid "$pid_file"; then
        local pid=$(cat "$pid_file")
        echo "$component is running (PID: $pid)."
        echo "Logs: $log_file"
    else
        echo "$component is not running."
    fi
}

# 启动所有组件（按顺序：gateway -> dispatcher -> worker -> client）
start_all() {
    # 启动 gateway
    start_component "gateway" || {
        echo "Failed to start gateway. Cleaning up..."
        stop_all_started
        exit 1
    }

    # 启动 dispatcher
    start_component "dispatcher" || {
        echo "Failed to start dispatcher. Cleaning up..."
        stop_all_started
        exit 1
    }

    # 启动 worker
    start_component "worker" || {
        echo "Failed to start worker. Cleaning up..."
        stop_all_started
        exit 1
    }

    # 等待 1 秒
    echo "Waiting 1 second before starting client..."
    sleep 1

    # 启动 client
    start_component "client" || {
        echo "Failed to start client. Cleaning up..."
        stop_all_started
        exit 1
    }
}

# 处理所有组件
handle_all() {
    local command=$1
    case "$command" in
        build)
            build_component "all"
            ;;
        start)
            start_all
            ;;
        stop)
            for comp in client worker dispatcher gateway; do
                stop_component "$comp"
            done
            ;;
        restart)
            for comp in client worker dispatcher gateway; do
                stop_component "$comp"
            done
            sleep 1
            build_component "all"
            start_all
            ;;
        status)
            for comp in gateway dispatcher worker client; do
                status_component "$comp"
            done
            ;;
    esac
}

# 主逻辑
valid_component "$COMPONENT"
case "$COMMAND" in
    build)
        if [ "$COMPONENT" = "all" ]; then
            build_component "all"
        else
            build_component "$COMPONENT"
        fi
        ;;
    start)
        if [ "$COMPONENT" = "all" ]; then
            handle_all "start"
        else
            start_component "$COMPONENT" || {
                echo "Failed to start $COMPONENT. Cleaning up..."
                stop_all_started
                exit 1
            }
        fi
        ;;
    stop)
        if [ "$COMPONENT" = "all" ]; then
            handle_all "stop"
        else
            stop_component "$COMPONENT"
        fi
        ;;
    restart)
        if [ "$COMPONENT" = "all" ]; then
            handle_all "restart"
        else
            restart_component "$COMPONENT"
        fi
        ;;
    status)
        if [ "$COMPONENT" = "all" ]; then
            handle_all "status"
        else
            status_component "$COMPONENT"
        fi
        ;;
    *)
        usage
        ;;
esac

exit 0