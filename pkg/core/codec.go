package core

import (
	"errors"
	"io"
)

// 包（frame）相关错误。
var (
	// ErrNeedMore 表示当前缓冲区数据不足以构成一个完整包，需要等待更多数据。
	// 这是适配器切分时"暂时无包"的语义，区别于数据错误。
	ErrNeedMore = errors.New("touchsocket: need more data")
	// ErrFrameTooLarge 表示从网络收到的长度字段超过合理上限，应丢弃/断开连接。
	ErrFrameTooLarge = errors.New("touchsocket: frame length exceeds limit")
)

// Codec 定义了从字节流中切分出完整数据包的接口。
//
// 这是 TouchSocket 中 "DataHandlingAdapter" 的 Go 惯用对应物。
// 实现者负责定义"什么是一个完整包"，框架负责在每次收到网络字节后调用 Peek。
//
// 典型实现：
//   - LengthPrefixCodec：4 字节长度前缀 + 定长 payload（最通用）
//   - FixedLengthCodec：定长包
//   - DelimiterCodec：以某个终止符/包头序列分割（文本协议）
//
// 实现约束：
//   - Peek 不得修改传入的 buffer（除移动读指针外不得破坏数据）。
//   - 数据不足时必须返回 ErrNeedMore，不得部分消费数据。
//   - 必须对来自网络的数量字段做上限校验（防止恶意 0xFFFFFFFF 导致 OOM）。
type Codec interface {
	// Peek 从 buf 中切分出一个完整包。
	// 成功时返回包内容（拷贝，调用者可安全持有），并将 buf 的读指针移动到包之后。
	// 数据不足时返回 ErrNeedMore，且不得移动读指针。
	// 包长度超过 Limit() 时返回 ErrFrameTooLarge。
	Peek(buf *Block) ([]byte, error)

	// Name 返回编解码器名称（用于日志与调试）。
	Name() string
}

// 内部：校验并读取长度前缀（大端）。
func readLength(buf *Block, headerLen int) (int, error) {
	if buf.Length() < headerLen {
		return 0, ErrNeedMore
	}
	if headerLen == 1 {
		return int(buf.Data()[0]), nil
	}
	// 大端读取前 headerLen 个字节
	v := 0
	d := buf.Data()
	for i := 0; i < headerLen; i++ {
		v = (v << 8) | int(d[i])
	}
	return v, nil
}

// 内部：将 int 写为前 headerLen 个大端字节。
func writeLength(headerLen int, v int) []byte {
	buf := make([]byte, headerLen)
	for i := headerLen - 1; i >= 0; i-- {
		buf[i] = byte(v & 0xFF)
		v >>= 8
	}
	return buf
}

// LengthPrefixCodec 是"长度前缀"编解码器。
//
// 包格式（默认）：[ N 字节长度(大端) ][ payload ]
// HeaderLength 指定长度字段的字节数（1/2/3/4），默认 4。
//
// 这是工业协议（Modbus 帧、自定义 TCP 协议）最常用的分帧方式。
type LengthPrefixCodec struct {
	HeaderLength int  // 长度字段字节数，默认 4
	MaxFrameSize int  // 允许的单个包 payload 最大长度，默认 1MB
	Name_        string // 可选自定义名称
}

// NewLengthPrefixCodec 创建默认 4 字节大端长度前缀的编解码器。
func NewLengthPrefixCodec() *LengthPrefixCodec {
	return &LengthPrefixCodec{HeaderLength: 4, MaxFrameSize: 1 << 20}
}

// WithHeaderLength 设置长度字段字节数（1-4）。
func (c *LengthPrefixCodec) WithHeaderLength(n int) *LengthPrefixCodec {
	if n < 1 || n > 4 {
		n = 4
	}
	c.HeaderLength = n
	return c
}

// WithMaxFrameSize 设置单包 payload 上限（默认 1MB）。
func (c *LengthPrefixCodec) WithMaxFrameSize(n int) *LengthPrefixCodec {
	if n > 0 {
		c.MaxFrameSize = n
	}
	return c
}

// WithName 设置自定义名称。
func (c *LengthPrefixCodec) WithName(name string) *LengthPrefixCodec {
	c.Name_ = name
	return c
}

// Name 实现 Codec 接口。
func (c *LengthPrefixCodec) Name() string {
	if c.Name_ != "" {
		return c.Name_
	}
	return "LengthPrefix(" + itoa(c.HeaderLength) + "B)"
}

// Peek 实现 Codec 接口。
func (c *LengthPrefixCodec) Peek(buf *Block) ([]byte, error) {
	hlen := c.HeaderLength
	if hlen == 0 {
		hlen = 4
	}
	if buf.Length() < hlen {
		return nil, ErrNeedMore
	}
	length, err := readLength(buf, hlen)
	if err != nil {
		return nil, err
	}
	if length < 0 || length > c.MaxFrameSize {
		// 超限：跳过这个（可能恶意的）头，避免死锁
		buf.Skip(hlen)
		return nil, ErrFrameTooLarge
	}
	if buf.Length() < hlen+length {
		// 有头但 payload 未收全
		// 头先消费掉，等下次 payload 来了再继续？不——需要保留头。
		// 正确做法：数据不足，整体保留，等更多数据。
		_ = length
		return nil, ErrNeedMore
	}
	// 数据充足：跳过头，读出 payload
	body := buf.Data()
	body = body[hlen:]
	// 拷贝 payload（调用者需持有）
	out := make([]byte, length)
	copy(out, body[:length])
	buf.Skip(hlen + length)
	return out, nil
}

// Encode 将 payload 编码为 [长度头 + payload]。
func (c *LengthPrefixCodec) Encode(payload []byte) []byte {
	hlen := c.HeaderLength
	if hlen == 0 {
		hlen = 4
	}
	header := writeLength(hlen, len(payload))
	out := make([]byte, hlen+len(payload))
	copy(out, header)
	copy(out[hlen:], payload)
	return out
}

// FixedLengthCodec 定长包编解码器。
type FixedLengthCodec struct {
	Length int
	Name_  string
}

// NewFixedLengthCodec 创建定长包编解码器。
func NewFixedLengthCodec(length int) *FixedLengthCodec {
	return &FixedLengthCodec{Length: length}
}

// Name 实现 Codec 接口。
func (c *FixedLengthCodec) Name() string {
	if c.Name_ != "" {
		return c.Name_
	}
	return "FixedLength(" + itoa(c.Length) + ")"
}

// Peek 实现 Codec 接口。
func (c *FixedLengthCodec) Peek(buf *Block) ([]byte, error) {
	if buf.Length() < c.Length {
		return nil, ErrNeedMore
	}
	out := make([]byte, c.Length)
	copy(out, buf.Data()[:c.Length])
	buf.Skip(c.Length)
	return out, nil
}

// Encode 将 payload 编码（不足补 0，超长截断）。
func (c *FixedLengthCodec) Encode(payload []byte) []byte {
	out := make([]byte, c.Length)
	copy(out, payload)
	return out
}

// DelimiterCodec 基于分隔符/包头序列的编解码器。
// 适用于文本协议（如以 \r\n 结尾的行、或 "AAA...BBB" 的定长头）。
type DelimiterCodec struct {
	// Prefix 是包的起始标记（可选，如 "AAA"）。若为 nil，则包从当前读指针开始。
	Prefix []byte
	// Suffix 是包的结束标记（必填，如 "\r\n"）。
	Suffix []byte
	// 包含 Suffix 在结果中。
	IncludeSuffix bool
	// MaxFrameSize 最大包长度保护。
	MaxFrameSize int
	Name_        string
}

// NewDelimiterCodec 创建终止符分割编解码器。
func NewDelimiterCodec(suffix []byte) *DelimiterCodec {
	return &DelimiterCodec{Suffix: suffix, MaxFrameSize: 1 << 20}
}

// WithPrefix 设置包头。
func (c *DelimiterCodec) WithPrefix(prefix []byte) *DelimiterCodec {
	c.Prefix = prefix
	return c
}

// WithIncludeSuffix 是否包含后缀在返回包内。
func (c *DelimiterCodec) WithIncludeSuffix(b bool) *DelimiterCodec {
	c.IncludeSuffix = b
	return c
}

// WithMaxFrameSize 设置上限。
func (c *DelimiterCodec) WithMaxFrameSize(n int) *DelimiterCodec {
	if n > 0 {
		c.MaxFrameSize = n
	}
	return c
}

// Name 实现 Codec 接口。
func (c *DelimiterCodec) Name() string {
	if c.Name_ != "" {
		return c.Name_
	}
	return "Delimiter(" + string(c.Suffix) + ")"
}

// Peek 实现 Codec 接口。
func (c *DelimiterCodec) Peek(buf *Block) ([]byte, error) {
	d := buf.Data()
	avail := len(d)
	if avail == 0 {
		return nil, ErrNeedMore
	}
	slen := len(c.Suffix)
	if slen == 0 {
		// 无后缀，返回全部可读数据
		out := make([]byte, avail)
		copy(out, d)
		buf.Reset()
		return out, nil
	}

	// 搜索后缀
	idx := -1
	searchFrom := 0
	if c.Prefix != nil {
		// 先搜索包头
		pi := indexOf(d, c.Prefix)
		if pi < 0 {
			// 无包头，保留可能的前缀片段
			keep := min(avail, len(c.Prefix)-1)
			if keep > 0 {
				_ = keep
			}
			return nil, ErrNeedMore
		}
		searchFrom = pi
	}
	idx = indexOf(d[searchFrom:], c.Suffix)
	if idx < 0 {
		// 保留可能的前缀（防止后缀被拆分）
		keep := min(avail-searchFrom, slen-1)
		_ = keep
		return nil, ErrNeedMore
	}
	end := searchFrom + idx + slen
	if c.IncludeSuffix {
		// 包含后缀
	} else {
		end -= slen
	}
	if end-searchFrom > c.MaxFrameSize {
		// 超限：跳过到下一个可能的包，避免死循环
		// 保守：直接跳过整个剩余，避免 OOM
		buf.Skip(avail)
		return nil, ErrFrameTooLarge
	}
	out := make([]byte, end-searchFrom)
	copy(out, d[searchFrom:end])
	buf.Skip(end)
	return out, nil
}

// Encode 将 payload 编码为 [Prefix + payload + Suffix]。
func (c *DelimiterCodec) Encode(payload []byte) []byte {
	out := make([]byte, 0, len(c.Prefix)+len(payload)+len(c.Suffix))
	out = append(out, c.Prefix...)
	out = append(out, payload...)
	if c.IncludeSuffix {
		out = append(out, c.Suffix...)
	}
	return out
}

// ---- 内部辅助 ----

// itoa 简单的 int->string，避免 fmt 开销。
func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	neg := n < 0
	if neg {
		n = -n
	}
	var buf [12]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		buf[i] = '-'
	}
	return string(buf[i:])
}

// 保留 io 引用以提示本包关注流式处理。
var _ = io.EOF
