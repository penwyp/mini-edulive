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
MODULE = github.com/penwyp/mini-edulive
VERSION = 0.1.0
BUILD_TIME = $(shell date +%Y-%m-%dT%H:%M:%S%z)
GIT_COMMIT = $(shell git rev-parse --short HEAD 2>/dev/null || echo "unknown")
GO_VERSION = $(shell go version | awk '{print $$3}')

# 编译标志
LDFLAGS = -ldflags "-X main.Version=$(VERSION) -X main.BuildTime=$(BUILD_TIME) -X main.GitCommit=$(GIT_COMMIT) -X main.GoVersion=$(GO_VERSION)"

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
build: deps protocol build-gateway build-client build-worker build-dispatcher

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
	@rm -f logs/edulive_gateway.log  # 清理日志文件
	$(BIN_DIR)/$(GATEWAY_BINARY_NAME)

.PHONY: run-client quic
run-client: build-client
	@rm -f logs/edulive_client.log  # 清理日志文件
	$(BIN_DIR)/$(CLIENT_BINARY_NAME)

.PHONY: run-worker
run-worker: build-worker
	@rm -f logs/edulive_worker.log  # Cleanup log file
	$(BIN_DIR)/$(WORKER_BINARY_NAME)

.PHONY: run-dispatcher quic
run-dispatcher: build-dispatcher
	@rm -f logs/edulive_dispatcher.log  # Cleanup log file
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

# 生成 Swagger 文档（假设使用 swag）
.PHONY: swagger
swagger:
	swag init -g $(GATEWAY_CMD_DIR)/main.go -o api/swagger

# 构建 Docker 镜像
.PHONY: docker-build
docker-build:
	$(DOCKER) build -t $(MODULE):$(VERSION) -f Dockerfile .

# 运行 Docker 容器
.PHONY: docker-run
docker-run: docker-build
	$(DOCKER) run -p 8080:8080 $(MODULE):$(VERSION)

# 性能测试（使用 wrk）
.PHONY: bench
bench: build
	$(BIN_DIR)/$(GATEWAY_BINARY_NAME) & \
	sleep 2; \
	$(WRK) -t10 -c100 -d30s http://localhost:8080/health; \
	pkill $(GATEWAY_BINARY_NAME)

# 安装工具（可选）
.PHONY: tools
tools:
	$(GO) install github.com/swaggo/swag/cmd/swag@latest
	$(GO) install github.com/golangci/golangci-lint/cmd/golangci-lint@latest
	# 如果 wrk 未安装，可手动安装：https://github.com/wg/wrk

# 显示版本信息
.PHONY: version
version:
	@echo "Version: $(VERSION)"
	@echo "Build Time: $(BUILD_TIME)"
	@echo "Git Commit: $(GIT_COMMIT)"
	@echo "Go Version: $(GO_VERSION)"

.PHONY: manage-test-start
manage-test-start:
	chmod +x ./test/manage_test_services.sh
	@echo "Starting test services via script..."
	@./test/manage_test_services.sh start

.PHONY: manage-test-stop
manage-test-stop:
	chmod +x ./test/manage_test_services.sh
	@echo "Stopping test services via script..."
	@./test/manage_test_services.sh stop

.PHONY: manage-test-status
manage-test-status:
	chmod +x ./test/manage_test_services.sh
	@echo "Checking test services status via script..."
	@./test/manage_test_services.sh status

.PHONY: manage-test-health
manage-test-health:
	chmod +x ./test/manage_test_services.sh
	@echo "Checking test services health via script..."
	@./test/manage_test_services.sh health

.PHONY: setup-consul
setup-consul:
	@echo "Checking if Consul is installed..."
	@#command -v consul >/dev/null 2>&1 || { echo "Consul not found. Please install Consul first."; exit 1; }
	@#echo "Starting Consul agent in dev mode..."
	@#consul agent -dev & \
#	sleep 2; \
	echo "Pushing load balancer rules to Consul KV Store..."; \
	curl -X PUT -d '{"/api/v1/user": ["http://localhost:8381", "http://localhost:8383"], "/api/v1/order": ["http://localhost:8382"]}' http://localhost:8300/v1/kv/edulive/loadbalancer/rules; \
	echo "Consul test environment setup complete."; \
	echo "Load balancer rules:"; \
	curl http://localhost:8300/v1/kv/edulive/loadbalancer/rules?raw

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
	@echo "Starting Redis..."
	@docker-compose -f test/docker/docker-compose.yml up -d mg-redis
	@echo "Starting Consul..."
	@docker-compose -f test/docker/docker-compose.yml up -d mg-consul
	@echo "Starting monitoring(Grafana|Prometheus|Jaeger)..."
	chmod +x test/docker/setup_grafana.sh
	chmod +x test/docker/setup_monitoring.sh
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
	@docker-compose -f test/docker/docker-compose.yml down
	@echo "外部依赖服务已停止"

.PHONY: protocol
protocol:
	@echo "Installing msgp tool..."
	@$(GO) install github.com/tinylib/msgp@latest || { echo "Failed to install msgp"; exit 1; }
	@echo "Running go generate for ./pkg/protocol..."
	@$(GO) generate ./pkg/protocol || { echo "Failed to run go generate"; exit 1; }
	@echo "Protocol code generation completed."

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