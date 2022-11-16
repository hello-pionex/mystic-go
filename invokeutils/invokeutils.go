package invokeutils

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"net/url"
	"os"

	"github.com/hello-pionex/mystic-go/code"
)

type Response struct {
	Result  bool            `json:"result"`
	Code    string          `json:"mcode,omitempty"`
	Data    json.RawMessage `json:"data,omitempty"`
	Message string          `json:"message,omitempty"`
}

var (
	MCODE_INVOKE_TIMEOUT = "INVOKE_TIMEOUT"
	MCODE_INVOKE_FAILED  = "INVOKE_FAILED"
)

// ExtractHeader 解析包中的错误码(该封装已经达成共识)
// 即：{result:true,mcode:"<code>",data:{}}
func ExtractHeader(name string, invokeErr error, statusCode int, res *Response, out interface{}) code.Error {
	if statusCode == 0 {
		// HTTP 调用过程出错
		urlErr, ok := invokeErr.(*url.Error)
		if ok {
			// 超时错误
			if urlErr.Timeout() {
				return code.NewMcode(MCODE_INVOKE_TIMEOUT, "http timeout")
			}

			if netErr, ok := urlErr.Err.(net.Error); ok {
				if netErr.Timeout() {
					return code.NewMcode(MCODE_INVOKE_TIMEOUT, "network timeout")
				}

				if netOpErr, ok := netErr.(*net.OpError); ok {
					if netOpErr.Timeout() {
						return code.NewMcode(MCODE_INVOKE_TIMEOUT, "network operation timeout")
					}

					if sysCallErr, ok := netOpErr.Err.(*os.SyscallError); ok {
						if sysCallErr.Syscall == "connectx" {
							return code.NewMcodef(MCODE_INVOKE_FAILED,
								"connect failed,addr=%v", netOpErr.Addr.String())
						}
						return code.NewMcode(MCODE_INVOKE_FAILED, "invoke failed for net problem")
					}
				}
			}

			return code.NewMcodef(MCODE_INVOKE_FAILED, "invoke error,%v", urlErr)
		}

		// 其他错误
		return code.NewMcode(
			MCODE_INVOKE_FAILED,
			invokeErr.Error(),
		)
	}

	// 返回状态出错
	if statusCode != http.StatusOK {
		return code.NewMcode(
			MCODE_INVOKE_FAILED,
			fmt.Sprintf("http status: %d", statusCode),
		)
	}

	// 处理结果出错
	if !res.Result {
		mcode := res.Code
		if mcode == "" {
			mcode = MCODE_INVOKE_FAILED
		}
		return code.NewMcode(mcode, res.Message)
	}

	// 无需解析结果
	if out == nil {
		return nil
	}

	err := json.Unmarshal(res.Data, out)
	if err != nil {
		return code.NewMcode(
			MCODE_INVOKE_FAILED,
			"parse return json payload failed",
		)
	}

	return nil
}

// ExtractHttpResponse 解析标准http.Response为输出
func ExtractHttpResponse(name string, invokeErr error, rsp *http.Response, out interface{}) code.Error {
	var commonResp Response
	var errCode code.Error

	if invokeErr == nil && rsp != nil {
		defer rsp.Body.Close()
	}

	statusCode := 0
	if rsp != nil {
		statusCode = rsp.StatusCode
	}

	if statusCode == http.StatusOK {
		body, err := ioutil.ReadAll(rsp.Body)
		if err != nil {
			errCode = code.NewMcode(MCODE_INVOKE_FAILED, "read response body failed")
			return errCode
		}

		if len(body) == 0 {
			errCode = code.NewMcode(MCODE_INVOKE_FAILED, "return json body empty")
			return errCode
		}

		err = json.Unmarshal(body, &commonResp)
		if err != nil {
			errCode = code.NewMcode(MCODE_INVOKE_FAILED, "return json body error")
			return errCode
		}
	}

	errCode = ExtractHeader(name, invokeErr, statusCode, &commonResp, out)
	return errCode
}

func NewParser(out interface{}) func(interface{}, *http.Response, error) error {
	return func(userData interface{}, rsp *http.Response, err error) error {
		return ExtractHttpResponse("", err, rsp, out)
	}
}
