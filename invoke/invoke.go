package invoke

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"time"

	"github.com/sirupsen/logrus"
)

type TemporaryError interface {
	Temporary() bool
}

type TimeoutError interface {
	Timeout() bool
}

func DefaultDoRequest(c *http.Client, r *http.Request) (*http.Response, error) {
	return c.Do(r)
}

func makeUrl(sche, host, path string, queries map[string][]string) (string, error) {
	b := make([]byte, 0, len(sche)+len(host)+len(path)+3+len(queries)*10)
	buffer := bytes.NewBuffer(b)

	buffer.WriteString(sche)
	buffer.WriteString("://")
	buffer.WriteString(host)
	buffer.WriteString(path)

	qv := url.Values{}
	for key, array := range queries {
		for _, value := range array {
			qv.Add(key, value)
		}
	}

	queryString := qv.Encode() // more performace? like appendEncode(&buffer,qv)

	if queryString != "" {
		buffer.WriteByte('?')
		buffer.WriteString(queryString)
	}

	return buffer.String(), nil
}

type Invoker struct {
	addr                  string
	path                  string
	method                string
	scheme                string
	headers               map[string]string
	queries               map[string][]string
	payload               func() ([]byte, error)
	client                *http.Client
	beforeRequest         func(*http.Request)
	afterRequests         []func(interface{}, *http.Response, error) error
	timeout               time.Duration
	doRequest             func(*http.Client, *http.Request) (*http.Response, error)
	logger                func(time.Duration, *http.Request, *http.Response, error)
	closeBodyAfterRequest bool
	parseStatus           func(*http.Response) error

	// field bellow will not copy
	ctx      context.Context
	userData interface{}
}

type InvokeError struct {
	Action      string
	Err         error
	IsTemporary bool
	IsTimeout   bool
}

func (invokeErr *InvokeError) Error() string {
	return fmt.Sprintf("Action:%s Err:%v", invokeErr.Action, invokeErr.Err)
}

func (invokeErr *InvokeError) Temporary() bool {
	return invokeErr.IsTemporary
}

func (invokeErr *InvokeError) Timeout() bool {
	return invokeErr.IsTimeout
}

func (invokeErr *InvokeError) Unwrap() error {
	return invokeErr.Err
}

func New() *Invoker {
	in := &Invoker{}
	in.logger = in.debugLogger
	return in
}

func Addr(addr string) *Invoker {
	return New().Addr(addr)
}

func Copy(invoker *Invoker) *Invoker {
	in := &Invoker{
		addr:                  invoker.addr,
		path:                  invoker.path,
		method:                invoker.method,
		scheme:                invoker.scheme,
		payload:               invoker.payload,
		client:                invoker.client,
		beforeRequest:         invoker.beforeRequest,
		afterRequests:         invoker.afterRequests,
		timeout:               invoker.timeout,
		doRequest:             invoker.doRequest,
		closeBodyAfterRequest: invoker.closeBodyAfterRequest,
		parseStatus:           invoker.parseStatus,
	}

	in.logger = in.debugLogger

	for key, value := range invoker.queries {
		if in.queries == nil {
			in.queries = make(map[string][]string, len(invoker.queries))
		}

		valueList := make([]string, len(value))
		copy(valueList, value)
		in.queries[key] = valueList
	}

	for key, value := range invoker.headers {
		if in.headers == nil {
			in.headers = make(map[string]string, len(invoker.headers))
		}
		in.headers[key] = value
	}

	return in
}

func (invoker *Invoker) Copy() *Invoker {
	return Copy(invoker)
}

func (invoker *Invoker) debugLogger(d time.Duration, req *http.Request, rsp *http.Response, err error) {
	func() {
		payload := []byte{}
		if invoker.payload != nil {
			payload, _ = invoker.payload()
		}

		if err != nil {
			if req == nil {
				logrus.WithError(err).Errorln("InvokeFailed")
				return
			}

			logrus.WithFields(logrus.Fields{
				"cost":    d,
				"method":  req.Method,
				"url":     req.URL.String(),
				"error":   err,
				"header":  req.Header,
				"payload": string(payload),
			}).Errorln("InvokeFailed")

			return
		}

		statusCode := 0
		if rsp != nil {
			statusCode = rsp.StatusCode
		}

		if statusCode != 200 {
			logrus.WithFields(logrus.Fields{
				"cost":    d,
				"method":  req.Method,
				"url":     req.URL.String(),
				"header":  req.Header,
				"error":   fmt.Sprintf("status=%d", statusCode),
				"status":  statusCode,
				"payload": string(payload),
			}).Errorln("InvokeFailed")

		} else {
			logrus.WithFields(logrus.Fields{
				"cost":    d,
				"method":  req.Method,
				"url":     req.URL.String(),
				"header":  req.Header,
				"status":  statusCode,
				"payload": string(payload),
			}).Infoln("InvokeDone")
		}
	}()
}

func (invoker *Invoker) Addr(addr string) *Invoker {
	invoker.addr = addr
	return invoker
}

func (invoker *Invoker) Method(method, path string) *Invoker {
	invoker.method = method
	invoker.path = path

	return invoker
}

func (invoker *Invoker) Delete(path string) *Invoker {
	return invoker.Method("DELETE", path)
}

func (invoker *Invoker) Get(path string) *Invoker {
	return invoker.Method("GET", path)
}

func (invoker *Invoker) Post(path string) *Invoker {
	return invoker.Method("POST", path)
}

func (invoker *Invoker) QueryValue(key string, value interface{}) *Invoker {
	return invoker.Query(key, fmt.Sprintf("%v", value))
}

func (invoker *Invoker) Query(key string, value interface{}) *Invoker {
	if invoker.queries == nil {
		invoker.queries = make(map[string][]string)
	}
	invoker.queries[key] = append(invoker.queries[key], fmt.Sprintf("%v", value))

	return invoker
}

func (invoker *Invoker) Timestamp() *Invoker {
	return invoker.Query("timestamp", time.Now().UnixNano()/int64(time.Millisecond))
}

func (invoker *Invoker) Header(key, value string) *Invoker {
	if invoker.headers == nil {
		invoker.headers = make(map[string]string)
	}
	invoker.headers[key] = value

	return invoker
}

func (invoker *Invoker) HeaderValue(key string, value interface{}) *Invoker {
	return invoker.Header(key, fmt.Sprintf("%v", value))
}

func (invoker *Invoker) Payload(body []byte) *Invoker {
	invoker.payload = func() ([]byte, error) {
		return body, nil
	}

	return invoker
}

func (invoker *Invoker) Json(v interface{}) *Invoker {
	b, err := json.Marshal(v)
	if err != nil {
		err = &InvokeError{"MarshalJson", err, false, false}
	}

	invoker.payload = func() ([]byte, error) {
		return b, err
	}

	return invoker
}

func (invoker *Invoker) DoRequest(doRequest func(c *http.Client, r *http.Request) (*http.Response, error)) *Invoker {
	invoker.doRequest = doRequest
	return invoker
}

func (invoker *Invoker) TLS() *Invoker {
	invoker.scheme = "https"
	return invoker
}

func (invoker *Invoker) Encoding(codec string) *Invoker {
	return invoker.Header("Content-Encoding", codec)
}

func (invoker *Invoker) EncodingGzip() *Invoker {
	return invoker.Encoding("gzip")
}

func (invoker *Invoker) Timeout(d time.Duration) *Invoker {
	invoker.timeout = d
	return invoker
}

func (invoker *Invoker) AutoCloseResponseBody() *Invoker {
	invoker.closeBodyAfterRequest = true
	return invoker
}

func (invoker *Invoker) UserData(data interface{}) *Invoker {
	invoker.userData = data
	return invoker
}

func (invoker *Invoker) Client(cli *http.Client) *Invoker {
	invoker.client = cli

	return invoker
}

func (invoker *Invoker) OnlyStatus200() *Invoker {
	invoker.CheckStatus(
		func(rsp *http.Response) error {
			if rsp.StatusCode != 200 {
				invoker.closeBodyAfterRequest = true
				return &InvokeError{
					Action: "ParseStatus",
					Err:    fmt.Errorf(rsp.Status),
				}
			}
			return nil
		},
	)
	return invoker
}

func (invoker *Invoker) CheckStatus(check func(rsp *http.Response) error) *Invoker {
	invoker.parseStatus = check
	return invoker
}

func (invoker *Invoker) Logger(f func(time.Duration, *http.Request, *http.Response, error)) {
	invoker.logger = f
}

func (invoker *Invoker) Context(ctx context.Context) *Invoker {
	invoker.ctx = ctx
	return invoker
}

func (invoker *Invoker) BeforeRequest(fn func(req *http.Request)) *Invoker {
	invoker.beforeRequest = fn
	return invoker
}

func (invoker *Invoker) Request() (*http.Response, error) {
	now := time.Now()

	reader := &bytes.Reader{}
	var err error

	if invoker.payload != nil {
		body, err := invoker.payload()
		if err != nil {
			if invoker.logger != nil {
				invoker.logger(0, nil, nil, err)
			}
			return nil, err
		}
		reader = bytes.NewReader(body)
	}

	if invoker.client == nil {
		invoker.client = DefaultHttpClient
	}

	var cancelFunc func()
	if invoker.ctx == nil {
		invoker.ctx = context.Background()
	}

	if invoker.timeout != 0 {
		invoker.ctx, cancelFunc = context.WithTimeout(invoker.ctx, invoker.timeout)
		defer cancelFunc()
	}
	if invoker.scheme == "" {
		invoker.scheme = "http"
	}

	urlString, _ := makeUrl(invoker.scheme, invoker.addr, invoker.path, invoker.queries)
	req, err := http.NewRequestWithContext(invoker.ctx, invoker.method, urlString, reader)
	if err != nil {
		return nil, &InvokeError{"BuildRequest", err, false, false}
	}

	for key, value := range invoker.headers {
		req.Header.Set(key, value)
	}

	if err != nil {
		if invoker.logger != nil {
			invoker.logger(0, nil, nil, err)
		}
		return nil, err
	}

	if invoker.beforeRequest != nil {
		invoker.beforeRequest(req)
	}

	if invoker.doRequest == nil {
		invoker.doRequest = DefaultDoRequest
	}

	rsp, err := invoker.doRequest(invoker.client, req)
	if err != nil {
		returnErr := &InvokeError{
			Action: "DoRequest",
			Err:    err,
		}
		te, ok := err.(TemporaryError)
		if ok {
			returnErr.IsTemporary = te.Temporary()
		}
		timeoutErr, ok := err.(TimeoutError)
		if ok {
			returnErr.IsTimeout = timeoutErr.Timeout()
		}
		err = returnErr
	}

	if rsp != nil && invoker.parseStatus != nil {
		err = invoker.parseStatus(rsp)
		if invoker.closeBodyAfterRequest && rsp.Body != nil {
			defer rsp.Body.Close()
		}
	}

	for _, afterRequest := range invoker.afterRequests {
		err = afterRequest(invoker.userData, rsp, err)
	}

	if invoker.logger != nil {
		invoker.logger(time.Since(now), req, rsp, err)
	}

	return rsp, err
}

func (invoker *Invoker) AfterRequest(fnList ...func(interface{}, *http.Response, error) error) *Invoker {
	invoker.afterRequests = fnList
	return invoker
}
