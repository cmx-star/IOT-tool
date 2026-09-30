package core

import "sync"

// Plugin 定义了插件接口，对齐 TouchSocket 的 IPlugin。
// 插件是 AOP 式扩展，每个通信环节（连接、接收、发送、断开）
// 都可以由多个插件拦截处理。
type Plugin interface {
	// PluginName 返回插件名称。
	PluginName() string
}

// PluginManager 管理一组插件，对齐 TouchSocket 的 PluginsManager。
type PluginManager struct {
	mu     sync.RWMutex
	plugins []Plugin
}

// NewPluginManager 创建空的插件管理器。
func NewPluginManager() *PluginManager {
	return &PluginManager{}
}

// Add 添加一个插件到管理器末尾。
func (pm *PluginManager) Add(plugin Plugin) {
	pm.mu.Lock()
	pm.plugins = append(pm.plugins, plugin)
	pm.mu.Unlock()
}

// Remove 按名称移除一个插件，返回是否找到。
func (pm *PluginManager) Remove(name string) bool {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	for i, p := range pm.plugins {
		if p.PluginName() == name {
			pm.plugins = append(pm.plugins[:i], pm.plugins[i+1:]...)
			return true
		}
	}
	return false
}

// All 返回所有插件的副本切片。
func (pm *PluginManager) All() []Plugin {
	pm.mu.RLock()
	defer pm.mu.RUnlock()
	out := make([]Plugin, len(pm.plugins))
	copy(out, pm.plugins)
	return out
}

// Contains 检查是否存在指定名称的插件。
func (pm *PluginManager) Contains(name string) bool {
	pm.mu.RLock()
	defer pm.mu.RUnlock()
	for _, p := range pm.plugins {
		if p.PluginName() == name {
			return true
		}
	}
	return false
}

// Count 返回插件数量。
func (pm *PluginManager) Count() int {
	pm.mu.RLock()
	defer pm.mu.RUnlock()
	return len(pm.plugins)
}
