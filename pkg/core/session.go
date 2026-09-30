package core

import (
	"sync"
	"sync/atomic"
)

// ServiceClient 是服务端会话的基类，对齐 TouchSocket 的 ServiceClientBase / SessionClient。
//
// 每个 TCP 客户端连接成功后，服务端会创建一个 ServiceClient 实例
// 来管理该连接的生命周期（收发数据、断开连接等）。
//
// 线程安全：Id 由框架分配；用户数据可通过 Data 访问（内部加锁）。
type ServiceClient struct {
	// Id 是连接的唯一标识，由服务端框架分配。
	Id int64
	// RemoteAddress 是远端地址。
	RemoteAddress string
	// LocalAddress 是本机地址。
	LocalAddress string

	// adapter 是数据适配器，用于粘包/拆包。
	adapter Adapter
	// adapterBlock 是适配器的工作缓冲区，累积未切分的原始字节。
	adapterBlock *Block

	// events 是事件路由器。
	events *EventRouter
	// plugins 是插件管理器。
	plugins *PluginManager

	// closed 是关闭标志。
	closed int32

	// data 是用户自定义数据存储（线程安全）。
	mu   sync.RWMutex
	data map[string]interface{}

	// sender 是底层发送函数，由框架注入。
	sender func([]byte) error
	// closer 是底层关闭函数，由框架注入。
	closer func()
}

// NewServiceClient 创建一个新的服务端会话。
func NewServiceClient(id int64, remoteAddr, localAddr string) *ServiceClient {
	return &ServiceClient{
		Id:            id,
		RemoteAddress: remoteAddr,
		LocalAddress:  localAddr,
		adapterBlock:  NewBlock(0),
		events:        NewEventRouter(),
		plugins:       NewPluginManager(),
		data:          make(map[string]interface{}),
	}
}

// Events 返回会话的事件路由器，用于订阅接收/断开等事件。
func (sc *ServiceClient) Events() *EventRouter { return sc.events }

// Plugins 返回会话的插件管理器。
func (sc *ServiceClient) Plugins() *PluginManager { return sc.plugins }

// SetAdapter 设置数据适配器。
func (sc *ServiceClient) SetAdapter(a Adapter) {
	sc.mu.Lock()
	sc.adapter = a
	sc.mu.Unlock()
}

// Adapter 返回当前数据适配器。
func (sc *ServiceClient) Adapter() Adapter {
	sc.mu.RLock()
	defer sc.mu.RUnlock()
	return sc.adapter
}

// FeedRaw 将原始网络字节送入适配器切分，切分出的完整包触发 Received 事件。
// 这是框架在每次从网络读到字节后调用的入口。
func (sc *ServiceClient) FeedRaw(raw []byte) {
	sc.mu.Lock()
	adapter := sc.adapter
	block := sc.adapterBlock
	sc.mu.Unlock()

	block.Append(raw)

	if adapter == nil {
		// 无适配器：整个可读数据直接触发 Received
		if block.Length() == 0 {
			return
		}
		pkg := block.ToArray()
		block.Reset()
		sc.events.Emit("Received", Event{Type: "Received", Context: pkg})
		return
	}

	// 循环切包：Adapter 会原地消费 Block 中已切分的数据
	for {
		pkg := adapter.Adapt(block)
		if pkg == nil {
			break
		}
		sc.events.Emit("Received", Event{Type: "Received", Context: pkg})
		if block.Length() == 0 {
			break
		}
	}
}

// Send 发送数据给客户端（通过框架注入的底层发送函数）。
func (sc *ServiceClient) Send(data []byte) error {
	if atomic.LoadInt32(&sc.closed) == 1 {
		return errClosed
	}
	sc.mu.Lock()
	sender := sc.sender
	sc.mu.Unlock()
	if sender == nil {
		return errNoSender
	}
	return sender(data)
}

// SendBytes 发送字节切片（便捷方法）。
func (sc *ServiceClient) SendBytes(data []byte) error {
	return sc.Send(data)
}

// Close 关闭该会话。
func (sc *ServiceClient) Close() {
	if !atomic.CompareAndSwapInt32(&sc.closed, 0, 1) {
		return
	}
	sc.events.Emit("Closed", Event{Type: "Closed"})
	sc.mu.Lock()
	closer := sc.closer
	sc.mu.Unlock()
	if closer != nil {
		closer()
	}
}

// IsClosed 返回是否已关闭。
func (sc *ServiceClient) IsClosed() bool {
	return atomic.LoadInt32(&sc.closed) == 1
}

// SetValue 存储用户自定义数据（线程安全）。
func (sc *ServiceClient) SetValue(key string, value interface{}) {
	sc.mu.Lock()
	sc.data[key] = value
	sc.mu.Unlock()
}

// GetValue 读取用户自定义数据。
func (sc *ServiceClient) GetValue(key string) (interface{}, bool) {
	sc.mu.RLock()
	v, ok := sc.data[key]
	sc.mu.RUnlock()
	return v, ok
}

// RemoveValue 移除用户自定义数据。
func (sc *ServiceClient) RemoveValue(key string) {
	sc.mu.Lock()
	delete(sc.data, key)
	sc.mu.Unlock()
}

// 错误定义
var (
	errClosed  = newError("connection closed")
	errNoSender = newError("no sender injected")
)

type simpleError struct{ msg string }

func (e *simpleError) Error() string { return e.msg }

func newError(msg string) error { return &simpleError{msg: msg} }

// FrameSender 是框架注入的底层发送函数签名。
type FrameSender func(data []byte) error

// 允许框架在内部使用 sc 设置 sender/closer（同包内直接赋值）
// 这里暴露 setter 以便同包内的网络层使用。

// SetSender 注入底层发送函数（仅框架内部使用）。
func (sc *ServiceClient) SetSender(s FrameSender) {
	sc.mu.Lock()
	sc.sender = s
	sc.mu.Unlock()
}

// SetCloser 注入底层关闭函数（仅框架内部使用）。
func (sc *ServiceClient) SetCloser(c func()) {
	sc.mu.Lock()
	sc.closer = c
	sc.mu.Unlock()
}
