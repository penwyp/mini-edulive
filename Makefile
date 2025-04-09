# 项目基本信息
GATEWAY_BINARY_NAME = mini-edulive-gateway
CLIENT_BINARY_NAME = mini-edulive-client
BIN_DIR = bin
GATEWAY_CMD_DIR = cmd/edulive-gateway
CLIENT_CMD_DIR = cmd/edulive-client
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
build: deps build-gateway build-client

.PHONY: build-gateway
build-gateway: deps
	@mkdir -p $(BIN_DIR)
	$(GO) build $(LDFLAGS) -o $(BIN_DIR)/$(GATEWAY_BINARY_NAME) $(GATEWAY_CMD_DIR)/main.go

.PHONY: build-client
build-client: deps
	@mkdir -p $(BIN_DIR)
	$(GO) build $(LDFLAGS) -o $(BIN_DIR)/$(CLIENT_BINARY_NAME) $(CLIENT_CMD_DIR)/main.go

# 运行项目
.PHONY: run-gateway
run-gateway: build-gateway
	@rm -f logs/edulive_gateway.log  # 清理日志文件
	$(BIN_DIR)/$(GATEWAY_BINARY_NAME)

.PHONY: run-client
run-client: build-client
	@rm -f logs/edulive_client.log  # 清理日志文件
	$(BIN_DIR)/$(CLIENT_BINARY_NAME)

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