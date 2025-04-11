package pool

import (
	"fmt"
	"sync"
)

// Resetter 定义对象重置接口
type Resetter interface {
	Reset()
}

// Pool 是一个类型安全的 sync.Pool 封装
type Pool[T any] struct {
	pool    sync.Pool
	newFunc func() T
}

// NewPool 创建一个新的类型安全池
func NewPool[T any](newFunc func() T) *Pool[T] {
	return &Pool[T]{
		newFunc: newFunc,
		pool: sync.Pool{
			New: func() interface{} {
				return newFunc()
			},
		},
	}
}

// Get 从池中获取对象
func (p *Pool[T]) Get() T {
	return p.pool.Get().(T)
}

// Put 归还对象到池中，如果对象实现了 Resetter，则先调用 Reset
func (p *Pool[T]) Put(obj T) {
	if resetter, ok := any(obj).(Resetter); ok {
		resetter.Reset()
	}
	p.pool.Put(obj)
}

// PoolManager 管理多个类型的池
type PoolManager struct {
	pools sync.Map // map[string]*Pool
}

// GlobalPoolManager 全局池管理器实例
var GlobalPoolManager = &PoolManager{}

// RegisterPool 注册一个新类型的池
func RegisterPool[T any](name string, newFunc func() T) *Pool[T] {
	pool := NewPool(newFunc)
	GlobalPoolManager.pools.Store(name, pool)
	return pool
}

// GetPool 获取指定类型的池
func GetPool[T any](name string) (*Pool[T], error) {
	pool, ok := GlobalPoolManager.pools.Load(name)
	if !ok {
		return nil, fmt.Errorf("pool %s not found", name)
	}
	typedPool, ok := pool.(*Pool[T])
	if !ok {
		return nil, fmt.Errorf("pool %s type mismatch", name)
	}
	return typedPool, nil
}
