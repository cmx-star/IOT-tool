package core

import "sync"

// Handler 是事件处理函数。
type Handler func(event Event)

// Event 是事件基类，承载事件类型和可选上下文。
type Event struct {
	Type    string
	Context interface{}
}

// EventRouter 管理事件处理器，对齐 TouchSocket 的事件订阅模型。
type EventRouter struct {
	mu      sync.RWMutex
	handlers map[string][]Handler
}

// NewEventRouter 创建空的事件路由器。
func NewEventRouter() *EventRouter {
	return &EventRouter{handlers: make(map[string][]Handler)}
}

// On 订阅指定类型的事件。
func (er *EventRouter) On(eventType string, handler Handler) {
	er.mu.Lock()
	er.handlers[eventType] = append(er.handlers[eventType], handler)
	er.mu.Unlock()
}

// Off 移除指定类型的事件处理器。返回移除数量。
func (er *EventRouter) Off(eventType string, handler Handler) int {
	er.mu.Lock()
	defer er.mu.Unlock()
	handlers := er.handlers[eventType]
	newLen := 0
	for _, h := range handlers {
		if h != handler {
			handlers[newLen] = h
			newLen++
		}
	}
	er.handlers[eventType] = handlers[:newLen]
	return len(handlers) - newLen
}

// Emit 触发指定类型的事件，按注册顺序调用所有处理器。
func (er *EventRouter) Emit(eventType string, event Event) {
	er.mu.RLock()
	handlers := make([]Handler, len(er.handlers[eventType]))
	copy(handlers, er.handlers[eventType])
	er.mu.RUnlock()

	for _, h := range handlers {
		h(event)
	}
}

// EventTypes 返回已注册的事件类型。
func (er *EventRouter) EventTypes() []string {
	er.mu.RLock()
	defer er.mu.RUnlock()
	types := make([]string, 0, len(er.handlers))
	for t := range er.handlers {
		types = append(types, t)
	}
	return types
}
