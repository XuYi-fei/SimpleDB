package parser

import (
	"SimpleDB/commons"
	"bytes"
	"errors"
)

type Tokenizer struct {
	// 输入的SQL语句
	stat []byte
	// 当前解析位置
	pos int
	// 当前token
	currentToken string
	// 标记是否需要刷新当前token
	flushToken bool
	// 解析过程中发生的异常
	err error
}

// NewTokenizer 构造函数，初始化输入的SQL语句
func NewTokenizer(stat []byte) *Tokenizer {
	return &Tokenizer{
		stat:         stat,
		pos:          0,
		currentToken: "",
		flushToken:   true,
		err:          nil,
	}
}

// Peek 返回当前token，如果有错误抛出异常
func (tokenizer *Tokenizer) Peek() (string, error) {
	if tokenizer.err != nil {
		return "", tokenizer.err
	}

	// 如果需要刷新token，则调用NextToken方法
	if tokenizer.flushToken {
		var token string
		token, err := tokenizer.next()
		if err != nil {
			tokenizer.err = err
			return "", err
		}
		tokenizer.currentToken = token
		tokenizer.flushToken = false
	}

	return tokenizer.currentToken, nil
}

// Pop 将当前的标记设置为需要刷新，这样下次调用peek()时会生成新的标记
func (tokenizer *Tokenizer) Pop() {
	tokenizer.flushToken = true
}

// ErrStat 返回带有错误位置标记的输入状态
func (tokenizer *Tokenizer) ErrStat() []byte {
	res := make([]byte, len(tokenizer.stat)+3)
	copy(res, tokenizer.stat[:tokenizer.pos])
	copy(res[tokenizer.pos:], []byte("<< "))
	copy(res[tokenizer.pos+3:], tokenizer.stat[tokenizer.pos:])
	return res
}

// popByte 移动到下一个字节
func (tokenizer *Tokenizer) popByte() {
	tokenizer.pos++
	if tokenizer.pos > len(tokenizer.stat) {
		tokenizer.pos = len(tokenizer.stat)
	}
}

// peekByte 查看当前字节，不移动位置
func (tokenizer *Tokenizer) peekByte() byte {
	if tokenizer.pos == len(tokenizer.stat) {
		return 0
	}
	return tokenizer.stat[tokenizer.pos]
}

// next 获取下一个token，如果有错误抛出异常
func (tokenizer *Tokenizer) next() (string, error) {
	if tokenizer.err != nil {
		return "", tokenizer.err
	}
	return tokenizer.nextMetaState()
}

// nextMetaState 获取下一个元状态。元状态可以是一个符号、引号包围的字符串或者一个由字母、数字或下划线组成的标记
func (tokenizer *Tokenizer) nextMetaState() (string, error) {
	for {
		b := tokenizer.peekByte()
		// 如果没有下一个字节，返回空字符串
		if b == 0 {
			return "", nil
		}
		// 如果下一个字节不是空白字符，跳出循环
		if !IsBlank(b) {
			break
		}
		// 否则，跳过这个字节
		tokenizer.popByte()
	}
	// 获取下一个字节
	b := tokenizer.peekByte()
	if IsSymbol(b) {
		// 如果这个字节是一个符号，跳过这个字节
		tokenizer.popByte()
		// 并返回这个符号
		return string(b), nil
	} else if b == '"' || b == '\'' {
		// 如果这个字节是引号，获取下一个引号状态
		return tokenizer.nextQuoteState()
	} else if IsAlphaBeta(b) || IsDigit(b) {
		// 如果这个字节是字母、数字或下划线，获取下一个标记状态
		return tokenizer.nextTokenState()
	} else {
		// 否则，设置错误状态为无效的命令异常
		tokenizer.err = errors.New(commons.ErrorMessage.InvalidCommandError)
		return "", tokenizer.err
	}

}

// nextTokenState 获取下一个标记。标记是由字母、数字或下划线组成的字符串。
func (tokenizer *Tokenizer) nextTokenState() (string, error) {
	// 创建一个buffer，用于存储标记
	var sb bytes.Buffer
	for {
		// 获取下一个字节
		b := tokenizer.peekByte()
		// 如果没有下一个字节，或者下一个字节不是字母、数字或下划线，那么结束循环
		if b == 0 || !(IsAlphaBeta(b) || IsDigit(b) || b == '_') {
			// 如果下一个字节是空白字符，那么跳过这个字节
			if b != 0 && IsBlank(b) {
				tokenizer.popByte()
			}
			// 返回标记
			return sb.String(), nil
		}
		// 如果下一个字节是字母、数字或下划线，那么将这个字节添加到buffer中
		sb.WriteByte(b)
		// 跳过这个字节
		tokenizer.popByte()
	}
}

// nextQuoteState 处理引号状态，即处理被引号包围的字符串。
func (tokenizer *Tokenizer) nextQuoteState() (string, error) {
	// 获取下一个字节，这应该是一个引号
	quote := tokenizer.peekByte()
	// 跳过这个引号
	tokenizer.popByte()
	// 创建一个buffer，用于存储被引号包围的字符串
	var sb bytes.Buffer
	for {
		// 获取下一个字节
		b := tokenizer.peekByte()
		if b == 0 {
			// 如果没有下一个字节，设置错误状态为无效的命令异常
			tokenizer.err = errors.New(commons.ErrorMessage.InvalidCommandError)
			return "", tokenizer.err
		}
		if b == quote {
			// 如果这个字节是引号，跳过这个字节，并跳出循环
			tokenizer.popByte()
			break
		}
		// 如果这个字节不是引号，将这个字节添加到StringBuilder中
		sb.WriteByte(b)
		// 跳过这个字节
		tokenizer.popByte()
	}
	return sb.String(), nil
}

// IsDigit 判断一个字节是否是数字
func IsDigit(b byte) bool {
	return b >= '0' && b <= '9'
}

// IsAlphaBeta 判断一个字节是否是字母
func IsAlphaBeta(b byte) bool {
	return (b >= 'a' && b <= 'z') || (b >= 'A' && b <= 'Z')

}

// IsSymbol 判断一个字节是否是符号
func IsSymbol(b byte) bool {
	return b == '>' || b == '<' || b == '=' || b == '*' ||
		b == ',' || b == '(' || b == ')'
}

// IsBlank 判断一个字节是否是空白字符
func IsBlank(b byte) bool {
	return b == '\n' || b == ' ' || b == '\t'
}
