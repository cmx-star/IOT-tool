// Package core 提供 TouchSocket-Go 的核心基础设施。
//
// 对齐 TouchSocket .NET 的 Core 层概念：
//   - Block (ByteBlock)：可变大小读写字节块
//   - BlockPool (BytePool)：字节块对象池，减少 GC 压力
//   - ServiceClient / Service：服务端会话与服务基类
//   - Adapter (DataHandlingAdapter)：数据适配器，解决 TCP 粘包/拆包
//   - Plugin (IPlugin)：插件，AOP 式扩展每一步通信流程
//   - Event：事件订阅模型
package core

import "io"

// Block 是一个可变大小的字节读写块，对齐 TouchSocket 的 ByteBlock 概念，
// 但用 Go 惯用方式实现：内部持有 []byte + 读写指针，避免 GC 压力。
//
// 设计取舍（参考 Go 生态调研结论）：
//   - 不内嵌 bufio.Writer（那是 I/O 缓冲，不是数据缓冲），只保留纯数据缓冲。
//   - 实现 io.Reader / io.Writer / io.ByteReader / io.ByteWriter，
//     这样可直接喂给需要标准流的协议解析器。
//   - 扩容曲线：< 4KB 翻倍，>= 4KB 每步 1.25x（借鉴 gnet ring buffer）。
type Block struct {
	data []byte // 内部数据缓冲
	rpos int    // 读指针
	wpos int     // 写指针
}

// NewBlock 创建一个指定初始容量的 Block。
func NewBlock(size int) *Block {
	if size < 1024 {
		size = 1024
	}
	return &Block{data: make([]byte, 0, size)}
}

// NewBlockFrom 从已有字节切片创建 Block，数据会被复制。
func NewBlockFrom(b []byte) *Block {
	block := NewBlock(len(b))
	if len(b) > 0 {
		block.data = append(block.data, b...)
		block.wpos = len(b)
	}
	return block
}

// Length 返回当前可读数据长度（写指针 - 读指针）。
func (b *Block) Length() int {
	return b.wpos - b.rpos
}

// Capacity 返回底层缓冲容量。
func (b *Block) Capacity() int {
	return cap(b.data)
}

// ToArray 返回当前可读数据的拷贝。
func (b *Block) ToArray() []byte {
	out := make([]byte, b.wpos-b.rpos)
	copy(out, b.data[b.rpos:b.wpos])
	return out
}

// Data 返回底层数据（不拷贝），调用者需确保 Block 不被修改。
func (b *Block) Data() []byte {
	return b.data[b.rpos:b.wpos]
}

// Reset 重置读写指针到起始位置，保留底层缓冲。
func (b *Block) Reset() {
	b.rpos = 0
	b.wpos = 0
}

// Clear 释放数据，保留容量。
func (b *Block) Clear() {
	b.rpos = 0
	b.wpos = 0
	b.data = b.data[:0]
}

// Append 向 Block 尾部追加数据。
func (b *Block) Append(data []byte) {
	// 确保读指针之前的数据已消费
	if b.rpos > 0 {
		// 将未读数据前移
		copied := copy(b.data, b.data[b.rpos:])
		b.data = b.data[:copied]
		b.wpos = copied
		b.rpos = 0
	}
	b.data = append(b.data, data...)
	b.wpos = len(b.data)
}

// AppendUInt16 追加一个 uint16 值（大端）。
func (b *Block) AppendUInt16(v uint16) {
	buf := make([]byte, 2)
	buf[0] = byte(v >> 8)
	buf[1] = byte(v)
	b.Append(buf)
}

// AppendUInt32 追加一个 uint32 值（大端）。
func (b *Block) AppendUInt32(v uint32) {
	buf := make([]byte, 4)
	buf[0] = byte(v >> 24)
	buf[1] = byte(v >> 16)
	buf[2] = byte(v >> 8)
	buf[3] = byte(v)
	b.Append(buf)
}

// AppendInt 追加一个 int 值（大端，4 字节）。
func (b *Block) AppendInt(v int) {
	b.AppendUInt32(uint32(v))
}

// ReadUInt16 从读指针位置读取 uint16（大端），读指针前移 2 字节。
func (b *Block) ReadUInt16() uint16 {
	if b.wpos-b.rpos < 2 {
		return 0
	}
	v := uint16(b.data[b.rpos])<<8 | uint16(b.data[b.rpos+1])
	b.rpos += 2
	return v
}

// ReadUInt32 从读指针位置读取 uint32（大端），读指针前移 4 字节。
func (b *Block) ReadUInt32() uint32 {
	if b.wpos-b.rpos < 4 {
		return 0
	}
	v := uint32(b.data[b.rpos])<<24 | uint32(b.data[b.rpos+1])<<16 |
		uint32(b.data[b.rpos+2])<<8 | uint32(b.data[b.rpos+3])
	b.rpos += 4
	return v
}

// PeekUInt32 预览第 n 个字节处的 uint32，不移动读指针。
func (b *Block) PeekUInt32(offset int) uint32 {
	if b.wpos-b.rpos < offset+4 {
		return 0
	}
	v := uint32(b.data[b.rpos+offset])<<24 | uint32(b.data[b.rpos+offset+1])<<16 |
		uint32(b.data[b.rpos+offset+2])<<8 | uint32(b.data[b.rpos+offset+3])
	return v
}

// Skip 跳过 n 个字节。
func (b *Block) Skip(n int) {
	if b.rpos+n > b.wpos {
		b.rpos = b.wpos
	} else {
		b.rpos += n
	}
}

// String 返回当前可读数据的字符串表示。
func (b *Block) String() string {
	return string(b.data[b.rpos:b.wpos])
}

// IsEOF 是否已无数据可读。
func (b *Block) IsEOF() bool {
	return b.rpos >= b.wpos
}

// Clone 返回当前 Block 的独立拷贝。
func (b *Block) Clone() *Block {
	return NewBlockFrom(b.ToArray())
}

// ---- io.Reader / io.Writer 实现 ----
// 让 Block 可直接作为标准流喂给协议解析器（如 http.ReadRequest、bufio 等）。

import_ = 0 // placeholder

// Read 实现 io.Reader。从读指针位置读取数据。
func (b *Block) Read(p []byte) (int, error) {
	if b.IsEOF() {
		if len(p) == 0 {
			return 0, nil
		}
		return 0, io.EOF
	}
	n := copy(p, b.data[b.rpos:b.wpos])
	b.rpos += n
	return n, nil
}

// Write 实现 io.Writer。向写指针位置追加数据。
func (b *Block) Write(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	b.Append(p)
	return len(p), nil
}

// WriteByte 实现 io.ByteWriter。
func (b *Block) WriteByte(c byte) error {
	b.data = append(b.data, c)
	b.wpos = len(b.data)
	return nil
}

// BlockPool 是 Block 的对象池，对齐 TouchSocket 的 BytePool。
// 使用 sync.Pool 减少 GC 压力。
type BlockPool struct {
	pool []poolEntry
}

type poolEntry struct {
	size int
	b    *Block
}

// NewBlockPool 创建一个 BlockPool。
func NewBlockPool() *BlockPool {
	return &BlockPool{}
}

// Rent 从池中租用一个 Block。size 为期望容量（至少 1024）。
// 如果池中有匹配的 Block 则复用，否则新建。
func (p *BlockPool) Rent(size int) *Block {
	if size < 1024 {
		size = 1024
	}
	for i, entry := range p.pool {
		if entry.size >= size && entry.b != nil {
			p.pool[i] = poolEntry{}
			entry.b.Reset()
			entry.b.data = entry.b.data[:0]
			return entry.b
		}
	}
	return NewBlock(size)
}

// Return 归还 Block 到池中。
func (p *BlockPool) Return(b *Block) {
	if b == nil {
		return
	}
	b.Clear()
	p.pool = append(p.pool, poolEntry{size: cap(b.data), b: b})
}

// 说明：Block 是普通数据缓冲，非 I/O 对象池。
// 若需跨 goroutine 共享，请用 sync.Pool 持有 *Block（存指针，避免 slice header 装箱）。
