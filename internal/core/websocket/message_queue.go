package websocket

import (
	"context"
	"sync"

	"github.com/coder/websocket"
	"github.com/penwyp/mini-edulive/pkg/logger"
)

// QueueOptions 消息队列配置选项
type QueueOptions struct {
	QueueSize    int
	WorkerCount  int
	WorkerBuffer int
}

// DefaultQueueOptions 返回默认的队列选项
func DefaultQueueOptions() QueueOptions {
	return QueueOptions{
		QueueSize:    100, // 队列可以缓存100条消息
		WorkerCount:  10,  // 10个并行工作者
		WorkerBuffer: 10,  // 工作者池大小
	}
}

// MessageQueue 管理消息处理的队列和工作池
type MessageQueue struct {
	msgChan     chan []byte
	workerPool  chan struct{}
	workerCount int
	queueSize   int
}

// NewMessageQueue 创建新的消息队列
func NewMessageQueue(opts QueueOptions) *MessageQueue {
	return &MessageQueue{
		msgChan:     make(chan []byte, opts.QueueSize),
		workerPool:  make(chan struct{}, opts.WorkerBuffer),
		workerCount: opts.WorkerCount,
		queueSize:   opts.QueueSize,
	}
}

// EnqueueMessage 将消息加入队列
func (q *MessageQueue) EnqueueMessage(data []byte) bool {
	select {
	case q.msgChan <- data:
		return true
	default:
		return false
	}
}

// StartWorkers 启动工作者池处理消息
func (q *MessageQueue) StartWorkers(ctx context.Context, conn *websocket.Conn, connCtx *ConnectionContext, handler *MessageHandler) <-chan struct{} {
	var wg sync.WaitGroup
	done := make(chan struct{})

	// 启动工作池
	for i := 0; i < q.workerCount; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for data := range q.msgChan {
				q.workerPool <- struct{}{} // 获取工作槽
				handler.HandleMessage(ctx, conn, data, connCtx, workerID)
				<-q.workerPool // 释放工作槽
			}
		}(i)
	}

	// 当所有工作完成后关闭done通道
	go func() {
		// 当主循环退出时，关闭消息通道
		<-ctx.Done()
		close(q.msgChan)
		wg.Wait()
		close(done)
		logger.Debug("All workers have completed")
	}()

	return done
}
