package config

import (
	"fmt"
	"github.com/fsnotify/fsnotify"
	"github.com/penwyp/mini-edulive/pkg/protocol"
	"github.com/spf13/viper"
	"go.uber.org/zap"
	"gopkg.in/yaml.v2"
	"log"
	"os"
	"strings"
	"sync"
	"time"
)

var configMgr *ConfigManager

// ConfigManager 管理配置及其变更通知
type ConfigManager struct {
	config     *Config
	mutex      sync.RWMutex
	ConfigChan chan *Config // 用于通知配置变更
}

// Config 定义弹幕系统的配置结构体
type Config struct {
	Type          string        `mapstructure:"type"` // "gateway", "client", "worker" 或 "dispatcher"
	App           App           `mapstructure:"app"`
	Logger        Logger        `mapstructure:"logger"`
	WebSocket     WebSocket     `mapstructure:"websocket"`
	Kafka         Kafka         `mapstructure:"kafka"`
	Redis         Redis         `mapstructure:"redis"`
	Distributor   Distributor   `mapstructure:"distributor"`
	Observability Observability `mapstructure:"observability"`
	Plugin        Plugin        `mapstructure:"plugin"`
	Middleware    Middleware    `mapstructure:"middleware"`
	Performance   Performance   `mapstructure:"performance"`
	Client        Client        `mapstructure:"client"` // 客户端专用
}

// App 服务器配置
type App struct {
	Port    string `mapstructure:"port"`
	GinMode string `mapstructure:"ginMode"`
}

// Client 客户端专用配置
type Client struct {
	LiveID       uint64        `mapstructure:"liveID"`
	UserID       uint64        `mapstructure:"userID"`
	SendInterval time.Duration `mapstructure:"sendInterval"`
	MaxRetries   int           `mapstructure:"maxRetries"`
	Mode         string        `mapstructure:"mode"` // 客户端模式：send 或 create
}

// Logger 日志配置
type Logger struct {
	Level      string `mapstructure:"level"`
	FilePath   string `mapstructure:"filePath"`
	MaxSize    int    `mapstructure:"maxSize"`
	MaxBackups int    `mapstructure:"maxBackups"`
	MaxAge     int    `mapstructure:"maxAge"`
	Compress   bool   `mapstructure:"compress"`
}

// WebSocket WebSocket 配置
type WebSocket struct {
	Enabled         bool          `mapstructure:"enabled"`
	MaxConns        int           `mapstructure:"maxConns"`
	IdleTimeout     time.Duration `mapstructure:"idleTimeout"`
	ReadBuffer      int           `mapstructure:"readBuffer"`
	WriteBuffer     int           `mapstructure:"writeBuffer"`
	Endpoint        string        `mapstructure:"endpoint"`
	ProtocolVersion uint8         `mapstructure:"protocolVersion"`
}

// Kafka Kafka 配置
type Kafka struct {
	Brokers  []string `mapstructure:"brokers"`
	Topic    string   `mapstructure:"topic"`
	Balancer string   `mapstructure:"balancer"`
	GroupID  string   `mapstructure:"groupID"`
}

// Redis Redis 配置
type Redis struct {
	Addrs    []string `mapstructure:"addrs"`
	Password string   `mapstructure:"password"`
	DB       int      `mapstructure:"db"`
}

// Distributor 分发配置
type Distributor struct {
	QUIC QUIC `mapstructure:"quic"`
	CDN  CDN  `mapstructure:"cdn"`
}

// QUIC QUIC 协议配置
type QUIC struct {
	Enabled  bool   `mapstructure:"enabled"`
	Addr     string `mapstructure:"addr"`
	CertFile string `mapstructure:"certFile"`
	KeyFile  string `mapstructure:"keyFile"`
}

// CDN CDN 分发配置
type CDN struct {
	Enabled  bool   `mapstructure:"enabled"`
	Endpoint string `mapstructure:"endpoint"`
}

// Observability 可观测性配置
type Observability struct {
	Prometheus Prometheus `mapstructure:"prometheus"`
	Jaeger     Jaeger     `mapstructure:"jaeger"`
}

// Prometheus 配置
type Prometheus struct {
	Enabled      bool   `mapstructure:"enabled"`
	Path         string `mapstructure:"path"`
	HttpEndpoint string `mapstructure:"httpEndpoint"`
}

// Jaeger 追踪配置
type Jaeger struct {
	Enabled      bool    `mapstructure:"enabled"`
	Endpoint     string  `mapstructure:"endpoint"`
	HttpEndpoint string  `mapstructure:"httpEndpoint"`
	Sampler      string  `mapstructure:"sampler"`
	SampleRatio  float64 `mapstructure:"sampleRatio"`
}

// Plugin 插件配置
type Plugin struct {
	Dir     string   `mapstructure:"dir"`
	Plugins []string `mapstructure:"plugins"`
}

// Middleware 中间件配置
type Middleware struct {
	RateLimit bool `mapstructure:"rateLimit"`
	Tracing   bool `mapstructure:"tracing"`
}

// Performance 性能相关配置
type Performance struct {
	MemoryPool MemoryPool `mapstructure:"memoryPool"`
}

// MemoryPool 内存池配置
type MemoryPool struct {
	Enabled         bool `mapstructure:"enabled"`
	TargetsCapacity int  `mapstructure:"targetsCapacity"`
	RulesCapacity   int  `mapstructure:"rulesCapacity"`
}

// InitConfig 初始化配置并返回 ConfigManager
func InitConfig(configFile string) *ConfigManager {
	cfg := &Config{}

	v := viper.New()
	v.SetConfigFile(configFile)
	v.SetConfigType("yaml")
	setDefaultValues(v)

	if err := v.ReadInConfig(); err != nil {
		log.Fatalf("Failed to read configuration file, %v", err)
	}
	if err := v.Unmarshal(cfg); err != nil {
		log.Fatalf("Failed to unmarshal configuration, %v", err)
	}

	if err := validateConfig(cfg); err != nil {
		log.Fatalf("Configuration validation failed, %v", err)
	}

	configMgr = &ConfigManager{
		config:     cfg,
		ConfigChan: make(chan *Config, 1),
		mutex:      sync.RWMutex{},
	}

	// 监听配置文件变化以实现热更新
	v.WatchConfig()
	v.OnConfigChange(func(e fsnotify.Event) {
		log.Printf("Configuration file changed, file:%s", e.Name)
		newCfg := &Config{}

		newV := viper.New()
		newV.SetConfigFile(e.Name)
		newV.SetConfigType("yaml")
		setDefaultValues(newV)

		if err := newV.ReadInConfig(); err != nil {
			log.Fatalf("Failed to read configuration file", zap.Error(err))
			return
		}
		if err := newV.Unmarshal(newCfg); err != nil {
			log.Fatalf("Failed to unmarshal configuration", zap.Error(err))
			return
		}

		if err := validateConfig(newCfg); err != nil {
			log.Fatalf("Configuration validation failed on reload", zap.Error(err))
			return
		}

		configMgr.mutex.Lock()
		configMgr.config = newCfg
		configMgr.mutex.Unlock()

		// 通知配置变更
		select {
		case configMgr.ConfigChan <- newCfg:
			log.Println("Configuration reload notification sent")
		default:
			log.Println("Config channel full, skipping notification")
		}
	})

	return configMgr
}

// GetConfig 获取当前配置（线程安全）
func (cm *ConfigManager) GetConfig() *Config {
	cm.mutex.RLock()
	defer cm.mutex.RUnlock()
	return cm.config
}

// GetConfig 获取当前全局配置实例（线程安全）
func GetConfig() *Config {
	return configMgr.GetConfig()
}

// SetConfig 设置当前全局配置实例（线程安全）
func SetConfig(c *Config) {
	configMgr.mutex.Lock()
	defer configMgr.mutex.Unlock()
	configMgr.config = c
}

// UpdateConfig 更新配置并通知监听者
func (cm *ConfigManager) UpdateConfig(cfg *Config) {
	cm.mutex.Lock()
	defer cm.mutex.Unlock()
	cm.config = cfg
	cm.ConfigChan <- cfg
}

// SaveConfigToFile 将配置保存到文件并保证字段顺序
func (cm *ConfigManager) SaveConfigToFile(cfg *Config, filePath string) error {
	cm.mutex.Lock()
	defer cm.mutex.Unlock()

	orderedData := yaml.MapSlice{
		{Key: "type", Value: cfg.Type},
		{Key: "app", Value: cfg.App},
		{Key: "logger", Value: cfg.Logger},
		{Key: "websocket", Value: cfg.WebSocket},
		{Key: "kafka", Value: cfg.Kafka},
		{Key: "redis", Value: cfg.Redis},
		{Key: "distributor", Value: cfg.Distributor},
		{Key: "observability", Value: cfg.Observability},
		{Key: "plugin", Value: cfg.Plugin},
		{Key: "middleware", Value: cfg.Middleware},
		{Key: "performance", Value: cfg.Performance},
		{Key: "client", Value: cfg.Client},
	}

	out, err := yaml.Marshal(orderedData)
	if err != nil {
		return err
	}

	if err := os.WriteFile(filePath, out, 0644); err != nil {
		return err
	}

	log.Println("Configuration saved to file", zap.String("path", filePath))
	return nil
}

// setDefaultValues 设置默认配置值
func setDefaultValues(v *viper.Viper) {
	v.SetDefault("type", "gateway")
	v.SetDefault("app.port", "8483")
	v.SetDefault("app.ginMode", "release")

	v.SetDefault("logger.level", "info")
	v.SetDefault("logger.filePath", "logs/bullet.log")
	v.SetDefault("logger.maxSize", 100)
	v.SetDefault("logger.maxBackups", 10)
	v.SetDefault("logger.maxAge", 30)
	v.SetDefault("logger.compress", true)

	v.SetDefault("websocket.enabled", true)
	v.SetDefault("websocket.maxConns", 1000000)
	v.SetDefault("websocket.idleTimeout", 5*time.Minute)
	v.SetDefault("websocket.readBuffer", 1024)
	v.SetDefault("websocket.writeBuffer", 1024)
	v.SetDefault("websocket.endpoint", "ws://localhost:8483")
	v.SetDefault("websocket.protocolVersion", protocol.CurrentVersion)

	v.SetDefault("kafka.brokers", []string{"localhost:9092"})
	v.SetDefault("kafka.topic", "bullet_topic")
	v.SetDefault("kafka.balancer", "hash")

	v.SetDefault("redis.addrs", []string{"localhost:8479", "localhost:8480", "localhost:8481"})
	v.SetDefault("redis.password", "")
	v.SetDefault("redis.db", 0)

	v.SetDefault("distributor.quic.enabled", true)
	v.SetDefault("distributor.quic.addr", "localhost:8484")
	v.SetDefault("distributor.cdn.enabled", false)
	v.SetDefault("distributor.cdn.endpoint", "cdn.example.com")

	v.SetDefault("observability.prometheus.enabled", true)
	v.SetDefault("observability.prometheus.path", "/metrics")
	v.SetDefault("observability.prometheus.httpEndpoint", "localhost:9090")
	v.SetDefault("observability.jaeger.enabled", false)
	v.SetDefault("observability.jaeger.endpoint", "localhost:6831")
	v.SetDefault("observability.jaeger.httpEndpoint", "localhost:14268")
	v.SetDefault("observability.jaeger.sampler", "always")
	v.SetDefault("observability.jaeger.sampleRatio", 1.0)

	v.SetDefault("plugin.dir", "bin/plugins")
	v.SetDefault("plugin.plugins", []string{"filter", "rate_limit"})

	v.SetDefault("middleware.rateLimit", true)
	v.SetDefault("middleware.tracing", true)

	v.SetDefault("performance.memoryPool.enabled", true)
	v.SetDefault("performance.memoryPool.targetsCapacity", 100)
	v.SetDefault("performance.memoryPool.rulesCapacity", 100)

	// client
	v.SetDefault("client.userID", 10001)
	v.SetDefault("client.sendInterval", 100*time.Millisecond)
	v.SetDefault("client.maxRetries", 3)
}

// validateConfig 验证配置有效性
// validateConfig 验证配置有效性
func validateConfig(cfg *Config) error {
	// 检查配置类型
	if err := checkConfigType(cfg.Type); err != nil {
		return err
	}

	// 公共验证
	if err := validateWebSocket(cfg.WebSocket); err != nil {
		return err
	}

	// 类型特定验证
	typeValidators := map[string]func(*Config) error{
		"gateway":    validateGateway,
		"client":     validateClient,
		"worker":     validateWorker,
		"dispatcher": validateDispatcher,
	}

	if validator, exists := typeValidators[cfg.Type]; exists {
		return validator(cfg)
	}
	return nil // 不应该发生，因为类型已在 checkConfigType 中验证
}

// checkConfigType 验证配置类型
func checkConfigType(configType string) error {
	validTypes := map[string]bool{
		"gateway":    true,
		"client":     true,
		"worker":     true,
		"dispatcher": true,
	}
	if !validTypes[configType] {
		return fmt.Errorf("invalid config type: %s, must be 'gateway', 'client', 'worker', or 'dispatcher'", configType)
	}
	return nil
}

// validateWebSocket 验证 WebSocket 配置（公共）
func validateWebSocket(ws WebSocket) error {
	if ws.IdleTimeout <= 0 {
		return fmt.Errorf("websocket idleTimeout must be positive: %s", ws.IdleTimeout)
	}
	if ws.ReadBuffer <= 0 || ws.WriteBuffer <= 0 {
		return fmt.Errorf("websocket readBuffer and writeBuffer must be positive: %d, %d", ws.ReadBuffer, ws.WriteBuffer)
	}
	if ws.Enabled && ws.MaxConns <= 0 {
		return fmt.Errorf("websocket maxConns must be positive when enabled: %d", ws.MaxConns)
	}
	return nil
}

// validateGateway 验证 gateway 类型配置
func validateGateway(cfg *Config) error {
	if len(cfg.Kafka.Brokers) == 0 {
		return fmt.Errorf("kafka brokers list cannot be empty")
	}
	if cfg.Kafka.Topic == "" {
		return fmt.Errorf("kafka topic cannot be empty")
	}
	if len(cfg.Redis.Addrs) == 0 {
		return fmt.Errorf("redis addrs cannot be empty")
	}
	if cfg.Distributor.QUIC.Enabled && cfg.Distributor.QUIC.Addr == "" {
		return fmt.Errorf("quic addr cannot be empty when enabled")
	}
	return nil
}

// validateClient 验证 client 类型配置
func validateClient(cfg *Config) error {
	if cfg.WebSocket.Endpoint == "" {
		return fmt.Errorf("websocket endpoint cannot be empty for client")
	}
	if !strings.HasPrefix(cfg.WebSocket.Endpoint, "ws://") && !strings.HasPrefix(cfg.WebSocket.Endpoint, "wss://") {
		return fmt.Errorf("websocket endpoint must start with ws:// or wss://: %s", cfg.WebSocket.Endpoint)
	}
	if cfg.Client.SendInterval <= 0 {
		return fmt.Errorf("client sendInterval must be positive: %s", cfg.Client.SendInterval)
	}
	if cfg.Client.MaxRetries < 0 {
		return fmt.Errorf("client maxRetries cannot be negative: %d", cfg.Client.MaxRetries)
	}
	if cfg.Client.Mode != "send" && cfg.Client.Mode != "create" {
		return fmt.Errorf("client mode must be 'send' or 'create': %s", cfg.Client.Mode)
	}
	return nil
}

// validateWorker 验证 worker 类型配置
func validateWorker(cfg *Config) error {
	if len(cfg.Kafka.Brokers) == 0 {
		return fmt.Errorf("kafka brokers list cannot be empty")
	}
	if cfg.Kafka.Topic == "" {
		return fmt.Errorf("kafka topic cannot be empty")
	}
	if cfg.Kafka.GroupID == "" {
		return fmt.Errorf("kafka groupID cannot be empty")
	}
	if len(cfg.Redis.Addrs) == 0 {
		return fmt.Errorf("redis addrs cannot be empty")
	}
	return nil
}

// validateDispatcher 验证 dispatcher 类型配置
func validateDispatcher(cfg *Config) error {
	if len(cfg.Redis.Addrs) == 0 {
		return fmt.Errorf("redis addrs cannot be empty for dispatcher")
	}
	if cfg.Distributor.QUIC.Enabled && cfg.Distributor.QUIC.Addr == "" {
		return fmt.Errorf("quic addr cannot be empty when enabled for dispatcher")
	}
	if cfg.App.Port == "" {
		return fmt.Errorf("app port cannot be empty for dispatcher")
	}
	return nil
}

// InitTestConfigManager 初始化测试配置管理器
func InitTestConfigManager() {
	configMgr = &ConfigManager{
		config: &Config{
			Type: "gateway",
			App: App{
				Port:    "8483",
				GinMode: "debug",
			},
			Logger: Logger{
				Level:    "debug",
				FilePath: "logs/bullet.log",
			},
			WebSocket: WebSocket{
				Enabled:     true,
				MaxConns:    1000,
				IdleTimeout: 1 * time.Minute,
			},
			Kafka: Kafka{
				Brokers: []string{"localhost:9092"},
				Topic:   "bullet_test",
			},
			Redis: Redis{
				Addrs:    []string{"localhost:8479", "localhost:8480", "localhost:8481"},
				Password: "",
			},
			Distributor: Distributor{
				QUIC: QUIC{
					Enabled: true,
					Addr:    "localhost:8484",
				},
			},
			Observability: Observability{
				Prometheus: Prometheus{Enabled: true},
			},
			Middleware: Middleware{
				RateLimit: true,
			},
		},
		ConfigChan: make(chan *Config, 1),
		mutex:      sync.RWMutex{},
	}
}
