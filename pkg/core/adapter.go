package core

// Adapter 定义了数据适配器接口，对齐 TouchSocket 的 DataHandlingAdapter。
//
// 适配器在每次网络收到原始字节流时调用 Adapt 方法，
// 将流中完整的数据包切分出来，解决 TCP 粘包/拆包问题。
// 如果当前字节流中没有完整包，Adapt 返回空切片，
// 剩余未处理字节会留存在 Block 中等待下次数据到达。
//
// 内置适配器（对齐 TouchSocket）：
//   - FixedHeaderAdapter：固定包头（如 \r\n\x0d\x0a）
//   - FixedLengthAdapter：固定长度包
//   - LengthPrefixAdapter：长度前缀包（4 字节长度 + 数据体）
//   - TerminatorAdapter：终止符分割
//   - JsonAdapter：JSON 字符串
//   - HttpAdapter：HTTP 请求/响应
type Adapter interface {
	// Adapt 从 data 中切分出一个完整包。
	// 返回切分出的数据；如果 data 中不足一个完整包，返回 nil。
	// 调用后 data 会被原地更新为剩余未处理字节（读指针前移）。
	Adapt(data *Block) []byte
}

// FixedHeaderAdapter 基于固定头部分割数据。
// 例如头为 "AA BB CC"，则包格式为: AA BB CC <payload>
type FixedHeaderAdapter struct {
	Header []byte
}

// NewFixedHeaderAdapter 创建固定包头适配器。
func NewFixedHeaderAdapter(header []byte) *FixedHeaderAdapter {
	return &FixedHeaderAdapter{Header: header}
}

// PluginName 实现 Plugin 接口。
func (a *FixedHeaderAdapter) PluginName() string { return "FixedHeaderAdapter" }

// Adapt 从 Block 中切分一个完整包（基于固定包头）。
// 如果当前数据不足以构成一个完整包，返回 nil。
func (a *FixedHeaderAdapter) Adapt(data *Block) []byte {
	prefix := a.Header
	if len(prefix) == 0 {
		// 无头，整个可读数据作为一个包
		pkg := data.ToArray()
		data.Reset()
		return pkg
	}
	// 在可读数据中搜索包头
	available := data.Data()
	hlen := len(prefix)
	if len(available) < hlen {
		return nil // 数据不足，等待更多数据
	}
	// 搜索包头在 available 中的位置
	idx := indexOf(available, prefix)
	if idx < 0 {
		// 当前没有完整包头，保留可能的前缀部分（防止包头被拆分）
		// 保留最多 len(prefix)-1 字节，等待更多数据
		keep := min(len(available), hlen-1)
		data.Skip(available[keep:]) // 这个逻辑需要改进
		return nil
	}
	// 从包头开始，到下一个包头（或末尾）为一个包
	start := idx
	// 查找下一个包头
	rest := available[start+hlen:]
	nextIdx := indexOf(rest, prefix)
	if nextIdx >= 0 {
		end := start + hlen + nextIdx
		pkg := make([]byte, end-start)
		copy(pkg, available[start:end])
		data.Skip(end)
		return pkg
	}
	// 没有找到下一个包头，到末尾都是一个包
	pkg := make([]byte, len(available)-start)
	copy(pkg, available[start:])
	data.Skip(len(available) - start)
	return pkg
}

// FixedLengthAdapter 基于固定包长度切分。
type FixedLengthAdapter struct {
	PackageLength int
}

// NewFixedLengthAdapter 创建固定长度包适配器。
func NewFixedLengthAdapter(length int) *FixedLengthAdapter {
	return &FixedLengthAdapter{PackageLength: length}
}

// PluginName 实现 Plugin 接口。
func (a *FixedLengthAdapter) PluginName() string { return "FixedLengthAdapter" }

// Adapt 从 Block 中切分一个固定长度包。
func (a *FixedLengthAdapter) Adapt(data *Block) []byte {
	if data.Length() < a.PackageLength {
		return nil
	}
	pkg := make([]byte, a.PackageLength)
	copy(pkg, data.Data()[:a.PackageLength])
	data.Skip(a.PackageLength)
	return pkg
}

// LengthPrefixAdapter 基于长度前缀切分。
// 包格式: [4字节长度(大端)] [payload]
type LengthPrefixAdapter struct {
	HeaderLength int // 长度前缀的字节数，默认 4
	ByteOrder    string // "BE" 或 "LE"，默认 "BE"
}

// NewLengthPrefixAdapter 创建长度前缀适配器。
func NewLengthPrefixAdapter() *LengthPrefixAdapter {
	return &LengthPrefixAdapter{HeaderLength: 4, ByteOrder: "BE"}
}

// PluginName 实现 Plugin 接口。
func (a *LengthPrefixAdapter) PluginName() string { return "LengthPrefixAdapter" }

// Adapt 从 Block 中切分一个长度前缀包。
func (a *LengthPrefixAdapter) Adapt(data *Block) []byte {
	hlen := a.HeaderLength
	if data.Length() < hlen {
		return nil
	}
	var bodyLen int
	if a.ByteOrder == "LE" {
		d := data.Data()
		for i := 0; i < hlen; i++ {
			bodyLen = (bodyLen << 8) | int(d[i])
		}
	} else {
		bodyLen = int(data.PeekUInt32(0) >> uint(32-hlen*8))
	}
	_ = hlen
	// 简化：假设 4 字节大端
	if a.HeaderLength == 4 && a.ByteOrder == "BE" {
		bodyLen = int(data.PeekUInt32(0))
	}
	if data.Length() < a.HeaderLength+bodyLen {
		return nil
	}
	data.Skip(a.HeaderLength)
	pkg := make([]byte, bodyLen)
	copy(pkg, data.Data()[:bodyLen])
	data.Skip(bodyLen)
	return pkg
}

// TerminatorAdapter 基于终止符切分。
type TerminatorAdapter struct {
	Terminator []byte
}

// NewTerminatorAdapter 创建终止符分割适配器。
func NewTerminatorAdapter(terminator []byte) *TerminatorAdapter {
	return &TerminatorAdapter{Terminator: terminator}
}

// PluginName 实现 Plugin 接口。
func (a *TerminatorAdapter) PluginName() string { return "TerminatorAdapter" }

// Adapt 从 Block 中切分一个以终止符结尾的包。
func (a *TerminatorAdapter) Adapt(data *Block) []byte {
	available := data.Data()
	tlen := len(a.Terminator)
	if tlen == 0 {
		pkg := available
		data.Reset()
		return pkg
	}
	// 搜索终止符
	idx := indexOf(available, a.Terminator)
	if idx < 0 {
		// 保留可能的前缀部分
		keep := min(len(available), tlen-1)
		_ = keep
		return nil
	}
	end := idx + tlen // 包含终止符
	pkg := make([]byte, end)
	copy(pkg, available[:end])
	data.Skip(end)
	return pkg
}

// JsonAdapter 基于 JSON 字符串切分（简单实现，按 {} 和 [] 配对）。
type JsonAdapter struct {
	// 预留，当前未实现完整 JSON 解析
}

// NewJsonAdapter 创建 JSON 适配器。
func NewJsonAdapter() *JsonAdapter {
	return &JsonAdapter{}
}

// PluginName 实现 Plugin 接口。
func (a *JsonAdapter) PluginName() string { return "JsonAdapter" }

// Adapt 简单实现：按平衡的 {} 或 [] 切分 JSON。
func (a *JsonAdapter) Adapt(data *Block) []byte {
	available := data.Data()
	if len(available) == 0 {
		return nil
	}
	// 跳过前导空白
	start := 0
	for start < len(available) && isJsonWhitespace(available[start]) {
		start++
	}
	if start >= len(available) {
		data.Skip(start)
		return nil
	}
	// 找到匹配的结束括号
	depth := 0
	inString := false
	escaped := false
	for i := start; i < len(available); i++ {
		c := available[i]
		if inString {
			if escaped {
				escaped = false
			} else if c == '\\' {
				escaped = true
			} else if c == '"' {
				inString = false
			}
			continue
		}
		switch c {
		case '"':
			inString = true
		case '{', '[':
			depth++
		case '}', ']':
			depth--
			if depth == 0 {
				end := i + 1
				pkg := make([]byte, end-start)
				copy(pkg, available[start:end])
				data.Skip(end)
				return pkg
			}
		}
	}
	// 未找到完整 JSON，保留已有数据
	return nil
}

// HttpAdapter 基于 HTTP 协议切分（预留，需配合 HTTP 解析器）。
type HttpAdapter struct{}

// NewHttpAdapter 创建 HTTP 适配器。
func NewHttpAdapter() *HttpAdapter {
	return &HttpAdapter{}
}

// PluginName 实现 Plugin 接口。
func (a *HttpAdapter) PluginName() string { return "HttpAdapter" }

// Adapt HTTP 数据切分（预留，当前返回 nil 表示未实现）。
func (a *HttpAdapter) Adapt(data *Block) []byte {
	return nil
}

// ---- 内部辅助 ----

// indexOf 在 haystack 中搜索 needle 的第一个位置，返回 -1 表示未找到。
func indexOf(haystack, needle []byte) int {
	if len(needle) == 0 {
		return 0
	}
	if len(haystack) < len(needle) {
		return -1
	}
	for i := 0; i <= len(haystack)-len(needle); i++ {
		match := true
		for j := 0; j < len(needle); j++ {
			if haystack[i+j] != needle[j] {
				match = false
				break
			}
		}
		if match {
			return i
		}
	}
	return -1
}

// min 返回两个 int 中的较小值。
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// isJsonWhitespace 判断字符是否为 JSON 空白字符。
func isJsonWhitespace(c byte) bool {
	return c == ' ' || c == '\t' || c == '\n' || c == '\r'
}
