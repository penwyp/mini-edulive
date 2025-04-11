# 项目基本信息
BIN_DIR = bin
GATEWAY_BINARY_NAME = mini-edulive-gateway
GATEWAY_CMD_DIR = cmd/edulive-gateway
CLIENT_CMD_DIR = cmd/edulive-client
CLIENT_BINARY_NAME = mini-edulive-client
WORKER_CMD_DIR = cmd/edulive-worker
WORKER_BINARY_NAME = mini-edulive-worker
DISPATCHER_CMD_DIR = cmd/edulive-dispatcher
DISPATCHER_BINARY_NAME = mini-edulive-dispatcher

CLIENT_CREATE_CONF_FILE = config/config_client_create.yaml

MODULE = github.com/penwyp/mini-edulive
VERSION = 0.1.0
BUILD_TIME = $(shell date +%Y-%m-%dT%H:%M:%S%z)
GIT_COMMIT = $(shell git rev-parse --short HEAD 2>/dev/null || echo "unknown")
GO_VERSION = $(shell go version | awk '{print $$3}')

# 编译标志
#LDFLAGS = -ldflags "-X main.Version=$(VERSION) -X main.BuildTime=$(BUILD_TIME) -X main.GitCommit=$(GIT_COMMIT) -X main.GoVersion=$(GO_VERSION)"
LDFLAGS = -ldflags "-X main.Version=$(VERSION) -X main.GoVersion=$(GO_VERSION)"

# 工具
GO = go
GOLINT = golangci-lint
DOCKER = docker
WRK = wrk

# 默认目标
.PHONY: all
all: build

# 安装依赖
.PHONY: deps
deps:
	$(GO) mod tidy
	$(GO) mod download

# 编译项目并将二进制放入 bin 目录
.PHONY: build
build: deps build-gateway build-client build-worker build-dispatcher

.PHONY: build-gateway
build-gateway: deps
	@mkdir -p $(BIN_DIR)
	$(GO) build $(LDFLAGS) -o $(BIN_DIR)/$(GATEWAY_BINARY_NAME) $(GATEWAY_CMD_DIR)/main.go

.PHONY: build-client
build-client: deps
	@mkdir -p $(BIN_DIR)
	$(GO) build $(LDFLAGS) -o $(BIN_DIR)/$(CLIENT_BINARY_NAME) $(CLIENT_CMD_DIR)/main.go

.PHONY: build-dispatcher
build-dispatcher: deps
	@mkdir -p $(BIN_DIR)
	$(GO) build $(LDFLAGS) -o $(BIN_DIR)/$(DISPATCHER_BINARY_NAME) $(DISPATCHER_CMD_DIR)/main.go

.PHONY: build-worker
build-worker: deps
	@mkdir -p $(BIN_DIR)
	$(GO) build $(LDFLAGS) -o $(BIN_DIR)/$(WORKER_BINARY_NAME) $(WORKER_CMD_DIR)/main.go

# 运行项目
.PHONY: run-gateway
run-gateway: build-gateway
	@echo '' > logs/edulive_gateway.log  # 清理日志文件
	$(BIN_DIR)/$(GATEWAY_BINARY_NAME)

.PHONY: run-client quic
run-client: build-client
	@echo '' > logs/edulive_client.log  # 清理日志文件
	$(BIN_DIR)/$(CLIENT_BINARY_NAME)

.PHONY: run-client-create quic
run-client-create: build-client
	@echo '' >logs/edulive_client.log  # 清理日志文件
	$(BIN_DIR)/$(CLIENT_BINARY_NAME) -config $(CLIENT_CREATE_CONF_FILE)

.PHONY: run-worker
run-worker: build-worker
	@echo '' >logs/edulive_worker.log  # Cleanup log file
	$(BIN_DIR)/$(WORKER_BINARY_NAME)

.PHONY: run-dispatcher quic
run-dispatcher: build-dispatcher
	@echo '' >logs/edulive_dispatcher.log  # Cleanup log file
	$(BIN_DIR)/$(DISPATCHER_BINARY_NAME)

# 测试
.PHONY: test
test:
	$(GO) test -v ./...

# 格式化代码
.PHONY: fmt
fmt:
	$(GO) fmt ./...
	gofmt -s -w .
	goimports -w .

# 检查代码质量
.PHONY: lint
lint:
	$(GOLINT) run ./...

# 清理编译产物
.PHONY: clean
clean:
	rm -rf $(BIN_DIR)
	$(GO) clean

# 安装工具（可选）
.PHONY: tools
tools:
	$(GO) install github.com/swaggo/swag/cmd/swag@latest
	$(GO) install github.com/golangci/golangci-lint/cmd/golangci-lint@latest
	# 如果 wrk 未安装，可手动安装：https://github.com/wg/wrk

.PHONY: prepare-proto
prepare-proto:
	mkdir -p proto/lib
	@if [ -d "proto/lib/googleapis" ]; then \
		echo "proto/lib/googleapis already exists, updating instead"; \
		cd proto/lib/googleapis && git pull; \
	else \
		git clone --depth 1 https://github.com/googleapis/googleapis.git proto/lib/googleapis; \
	fi
	@if [ -d "proto/lib/grpc-proto" ]; then \
		echo "proto/lib/grpc-proto already exists, updating instead"; \
		cd proto/lib/grpc-proto && git pull; \
	else \
		git clone --depth 1 https://github.com/grpc/grpc-proto.git proto/lib/grpc-proto; \
	fi

# 生成 protobuf 文件
.PHONY: proto
proto: prepare-proto
	protoc -I . \
		-I proto/lib/googleapis \
		-I proto/lib/grpc-proto \
		--go_out=./proto \
		--go_opt=paths=source_relative \
		--go-grpc_out=./proto \
		--go-grpc_opt=paths=source_relative \
		--grpc-edulive_out=./proto \
		--grpc-edulive_opt=paths=source_relative \
		--grpc-edulive_opt generate_unbound_methods=true \
		--plugin=protoc-gen-grpc-edulive=$(shell go env GOPATH)/bin/protoc-gen-grpc-edulive \
		./proto/hello.proto

# 启动测试环境所有外部依赖
.PHONY: start-test-env
start-test-env:
	@echo "Starting test environment..."
	@echo "Starting zookeeper/kafka/redis cluster/jaeger/prometheus/grafana..."
	@docker-compose -f test/docker/docker-compose.yaml up -d
	@echo "Setting up monitoring(Grafana|Prometheus|Jaeger)..."
	@chmod +x test/docker/setup_grafana.sh
	@chmod +x test/docker/setup_monitoring.sh
	@echo "Setting up kafka topic"
	@docker exec -it me-kafka kafka-topics --create --topic bullet_topic --partitions 3 --replication-factor 1 --bootstrap-server localhost:9092
	@./test/docker/setup_monitoring.sh

# 启动监控服务
.PHONY: setup-monitoring
setup-monitoring:
	chmod +x test/docker/setup_grafana.sh
	chmod +x test/docker/setup_monitoring.sh
	@./test/docker/setup_monitoring.sh

# 停止外部依赖服务
.PHONY: stop-test-env
stop-test-env:
	@docker-compose -f test/docker/docker-compose.yaml down
	@echo "外部依赖服务已停止"
	@docker volume rm -f docker_me-zookeeper-data
	@docker volume rm -f docker_me-kafka-data
	@docker volume rm -f docker_me-redis-node-1-data
	@docker volume rm -f docker_me-redis-node-2-data
	@docker volume rm -f docker_me-redis-node-3-data
	@docker volume rm -f docker_me-jaeger-data
	@docker volume rm -f docker_me-prometheus-data
	@docker volume rm -f docker_me-grafana-data
	@echo "外部依赖服务数据卷已删除"

.PHONY: quic
quic:
	@mkdir -p test/ssl
	@echo "Checking QUIC HTTPS certificates..."
	@if [ -f test/ssl/cert.pem ] && openssl x509 -checkend 0 -in test/ssl/cert.pem > /dev/null 2>&1; then \
		echo "Certificates already exist and are not expired. Skipping generation."; \
	else \
		echo "Generating QUIC HTTPS certificates..."; \
		openssl req -x509 -newkey rsa:4096 -keyout test/ssl/key.pem -out test/ssl/cert.pem -days 9999 -nodes -config test/ssl/san.cnf || { echo "Failed to generate certificates"; exit 1; }; \
		echo "Certificates generated: test/ssl/cert.pem, test/ssl/key.pem"; \
	fi

# 启动所有服务
.PHONY: start-all
start-all: build
	@echo "Starting all services..."
	@chmod +x scripts/control.sh
	@bash scripts/control.sh start all || { echo "Failed to start all services"; exit 1; }

# 停止所有服务
.PHONY: stop-all
stop-all:
	@echo "Stopping all services..."
	@chmod +x scripts/control.sh
	@bash scripts/control.sh stop all || { echo "Failed to stop all services"; exit 1; }

.PHONY: restart-all
restart-all: stop-all start-all

# 显示可访问的 HTTP 链接
.PHONY: links
links:
	@IP=$$(hostname -I 2>/dev/null | awk '{print $$1}' || ifconfig | grep 'inet ' | grep -v 127.0.0.1 | awk '{print $$2}' | head -n 1); \
	if [ -z "$$IP" ]; then \
		IP="127.0.0.1"; \
		echo "Warning: Could not detect external IP, falling back to 127.0.0.1"; \
	fi; \
	echo "Accessible HTTP links:"; \
	echo "  - Jaeger UI: http://$$IP:8430"; \
	echo "  - Jaeger OTLP HTTP: http://$$IP:8431"; \
	echo "  - Grafana: http://$$IP:8450/d/mini-edulive-monitoring (login: admin/admin123)"; \
	echo "  - Prometheus: http://$$IP:8490"