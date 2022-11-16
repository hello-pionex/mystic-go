package tinyutil

import (
	"bytes"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
)

type TextFormatter struct {
	// 让Msg的首字母大写
	UppercaseFirstMsgLetter bool

	// 内容部分排序
	DisableSorting bool

	// 没有数据使用指定字符括起来
	QuoteEmptyFields bool

	// 括住内容的起始字符
	StartQuoteCharacter string

	// 括住内容的起始字符
	EndQuoteCharacter string

	// 打印调用信息的最低等级，默认为警告
	PrintCallerLevel logrus.Level

	sync.Once
}

func (f *TextFormatter) init(entry *logrus.Entry) {
	if len(f.StartQuoteCharacter) == 0 {
		f.StartQuoteCharacter = "\""
	}
	if len(f.EndQuoteCharacter) == 0 {
		f.EndQuoteCharacter = "\""
	}

	if f.PrintCallerLevel == 0 {
		f.PrintCallerLevel = logrus.WarnLevel
	}
}

func (f *TextFormatter) Format(entry *logrus.Entry) ([]byte, error) {
	var b *bytes.Buffer
	if entry.Buffer != nil {
		b = entry.Buffer
	} else {
		b = &bytes.Buffer{}
	}

	f.Do(func() { f.init(entry) })

	// [DEBU][2018-12-28 19:58:50.052]
	levelText := strings.ToUpper(entry.Level.String())[0:4]
	timeText := entry.Time.Format("2006-01-02 15:04:05")
	timeMsPart := int64(entry.Time.Nanosecond()) / int64(time.Millisecond) // 单独处理毫秒是因为标准库的毫秒长度不定
	fmt.Fprintf(b, "[%s][%s.%03d]", levelText, timeText, timeMsPart)

	// 第一个字母大写
	if len(entry.Message) != 0 {
		if f.UppercaseFirstMsgLetter {
			b.WriteString(strings.ToUpper(entry.Message[:1]))
			b.WriteString(fmt.Sprintf("%-30s", entry.Message[1:]))
		} else {
			b.WriteString(fmt.Sprintf("%-31s", entry.Message))
		}
	} else {
		b.WriteString(fmt.Sprintf("%-31s", ""))
	}
	b.WriteByte(' ')
	if v, exist := entry.Data["accountId"]; exist {
		f.appendKeyValue(b, "accountId", v)
	}
	if v, exist := entry.Data["traceId"]; exist {
		f.appendKeyValue(b, "traceId", v)
	}
	if v, exist := entry.Data["traceName"]; exist {
		f.appendKeyValue(b, "traceName", v)
	}
	if v, exist := entry.Data["pkg"]; exist {
		f.appendKeyValue(b, "pkg", v)
	}
	if v, exist := entry.Data["function"]; exist {
		f.appendKeyValue(b, "function", v)
	}

	ingoredKeys := map[string]bool{
		"accountId": true,
		"traceId":   true,
		"traceName": true,
		"pkg":       true,
		"function":  true,
	}

	keys := make([]string, 0, len(entry.Data))
	for key := range entry.Data {
		_, exist := ingoredKeys[key]
		if exist {
			continue
		}
		keys = append(keys, key)
	}

	sort.Strings(keys)
	for _, key := range keys {
		f.appendKeyValue(b, key, entry.Data[key])
	}

	if entry.HasCaller() && entry.Level <= f.PrintCallerLevel {
		file := entry.Caller.File
		index := strings.IndexAny(file, "/src/")
		if index != -1 {
			file = "$GOPATH" + file[6:]
		}
		f.appendKeyValue(b, logrus.FieldKeyFile, fmt.Sprintf("%s:%d", file, entry.Caller.Line))
		f.appendKeyValue(b, logrus.FieldKeyFunc, fmt.Sprintf("%s", entry.Caller.Function))
	}

	b.WriteByte('\n')
	return b.Bytes(), nil
}

func (f *TextFormatter) needsQuoting(text string) bool {
	if f.QuoteEmptyFields && len(text) == 0 {
		return true
	}
	for _, ch := range text {
		if !((ch >= 'a' && ch <= 'z') ||
			(ch >= 'A' && ch <= 'Z') ||
			(ch >= '0' && ch <= '9') ||
			ch == '-' || ch == '.') {
			return true
		}
	}
	return false
}

func (f *TextFormatter) appendKeyValue(b *bytes.Buffer, key string, value interface{}) {
	b.WriteString(key)
	b.WriteByte('=')
	f.appendValue(b, value)
	b.WriteByte(' ')
}

func (f *TextFormatter) appendValue(b *bytes.Buffer, value interface{}) {
	switch value := value.(type) {
	case string:
		if !f.needsQuoting(value) {
			b.WriteString(value)
		} else {
			fmt.Fprintf(b, "%s%v%s", f.StartQuoteCharacter, value, f.EndQuoteCharacter)
		}
	case error:
		errmsg := value.Error()
		if !f.needsQuoting(errmsg) {
			b.WriteString(errmsg)
		} else {
			fmt.Fprintf(b, "%s%v%s", f.StartQuoteCharacter, errmsg, f.EndQuoteCharacter)
		}
	default:
		fmt.Fprint(b, value)
	}
}
