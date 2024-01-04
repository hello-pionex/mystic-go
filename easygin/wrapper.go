package easygin

import (
	"fmt"
	"runtime/debug"
	"sync"
	"sync/atomic"

	"time"

	_ "github.com/gin-contrib/gzip"
	"github.com/gin-gonic/gin"
	"github.com/hello-pionex/mystic-go/code"
	"github.com/sirupsen/logrus"

	"github.com/DeanThompson/ginpprof"
)

type Response struct {
	Result    bool        `json:"result"`
	Mcode     string      `json:"mcode,omitempty"`
	Code      string      `json:"code,omitempty"`
	Message   string      `json:"message,omitempty"`
	Data      interface{} `json:"data,omitempty"`
	Timestamp int64       `json:"timestamp"`
}

type NopResponse interface {
	NopResponse()
}

func NewErrorResponse(code code.Error) *Response {
	return &Response{
		Timestamp: time.Now().UnixNano() / int64(time.Millisecond),
		Message:   code.Message(),
	}
}

func NewOkResponse(data interface{}) *Response {
	return &Response{
		Result:    true,
		Timestamp: time.Now().UnixNano() / int64(time.Millisecond),
		Data:      data,
	}
}

type StatusError interface {
	HttpStatus() int
}

type Wrapper struct {
	pool sync.Pool
	cfg  *Config
}

func New(cfg *Config) *Wrapper {
	if cfg.log == nil {
		cfg.log = logrus.WithField("pkg", "easygin")
	}
	if cfg.logMode == nil {
		mode := LogTypeSuccess | LogTypeError
		cfg.logMode = &mode
	}

	w := &Wrapper{
		pool: sync.Pool{
			New: func() interface{} {
				return new(Response)
			},
		},
		cfg: cfg,
	}

	w.cfg.GinEngine.Use(w.LogNotProcess())

	return w
}

var (
	ErrExceedMaxPendingRequest = code.NewMcodef("EXCEED_MAX_PENDING_REQUEST", "exceed max pending request")
)

type WrappedFunc func(ctx *gin.Context) (interface{}, error)

type HttpServer interface {
	Handle(string, string, ...gin.HandlerFunc) gin.IRoutes
}

type Config struct {
	GinEngine *gin.Engine
	WrapOption
}

const (
	LogTypeOff = iota * 0x1000
	LogTypeSuccess
	LogTypeError
)

const (
	ErrorCodeFieldNameMcode = "mcode"
	ErrorCodeFieldNameCode  = "code"
)

type WrapOption struct {
	log                *logrus.Entry
	maxPendingRequests *int
	defaultErrorStatus *int
	logMode            *int
	onRecover          func(interface{}) error
	requestWeight      int
	convertError       func(err error) code.Error

	errorCodeFieldName string
}

func NewWrapOption() *WrapOption {
	return &WrapOption{}
}

func (opt *WrapOption) LogEntry(log *logrus.Entry) *WrapOption {
	opt.log = log
	return opt
}

func (opt *WrapOption) MaxPendingRequests(limit int) *WrapOption {
	opt.maxPendingRequests = &limit
	return opt
}

func (opt *WrapOption) ErrorStatus(status int) *WrapOption {
	opt.defaultErrorStatus = &status
	return opt
}

func (opt *WrapOption) LogMode(mode int) *WrapOption {
	opt.logMode = &mode
	return opt
}

func (opt *WrapOption) OnRecoverError(fn func(interface{}) error) *WrapOption {
	opt.onRecover = fn
	return opt
}

func (opt *WrapOption) ConvertError(fn func(error) code.Error) *WrapOption {
	opt.convertError = fn
	return opt
}

func (opt *WrapOption) ErrorCodeFieldName(fieldName string) *WrapOption {
	opt.errorCodeFieldName = fieldName
	return opt
}

func (opt *WrapOption) Merge(from *WrapOption) *WrapOption {
	if from.logMode != nil {
		opt.logMode = from.logMode
	}

	if from.onRecover != nil {
		opt.onRecover = from.onRecover
	}

	if from.maxPendingRequests != nil {
		opt.maxPendingRequests = from.maxPendingRequests
	}

	if from.log != nil {
		opt.log = nil
	}

	return opt
}

func (wrapper *Wrapper) SetupPprof(prefix string) {
	if prefix != "" {
		ginpprof.WrapGroup(wrapper.cfg.GinEngine.Group(prefix))
	} else {
		ginpprof.Wrapper(wrapper.cfg.GinEngine)
	}
}

func (wrapper *Wrapper) Wrap(f WrappedFunc, regPath string, options ...*WrapOption) gin.HandlerFunc {
	opt := wrapper.cfg.WrapOption // copy

	for _, option := range options {
		opt.Merge(option)
	}

	recoverFunc := opt.onRecover
	log := logrus.WithField("pkg", "easygin")
	if opt.log != nil {
		log = opt.log
	}
	maxPendingRequest := 100000
	if opt.maxPendingRequests != nil {
		maxPendingRequest = *opt.maxPendingRequests
	}

	requestWeight := 1
	if opt.requestWeight != 0 {
		requestWeight = opt.requestWeight
	}

	logMode := LogTypeError | LogTypeSuccess
	if opt.logMode != nil {
		logMode = *opt.logMode
	}
	if opt.errorCodeFieldName == "" {
		opt.errorCodeFieldName = ErrorCodeFieldNameMcode
	}

	onError := opt.convertError
	pendingRequests := int64(0)
	defaultErrorStatus := 200
	if opt.defaultErrorStatus != nil {
		defaultErrorStatus = *opt.defaultErrorStatus
	}

	return func(httpCtx *gin.Context) {
		if httpCtx.Keys == nil {
			httpCtx.Keys = make(map[string]interface{}, 1)
		}

		httpCtx.Keys["easygin"] = 1

		since := time.Now()
		var (
			data    interface{}
			err     error
			retErr  code.Error
			pending int64
		)

		defer func() {
			// 拦截业务层的异常
			if r := recover(); r != nil {
				if recoverFunc != nil {
					r = recoverFunc(r)
				}

				if codeErr, ok := r.(code.Error); ok {
					retErr = codeErr
				} else {
					retErr = code.NewMcode("INTERNAL_ERROR", "Service internal error")
					fmt.Println(r)
					fmt.Println(string(debug.Stack()))
				}
			}

			// 错误返回介入

			if err != nil {
				if onError != nil {
					retErr = onError(err)
				} else {
					if codeError, ok := err.(code.Error); ok {
						retErr = codeError
					} else {
						retErr = code.NewMcode("UNKNOWN_ERROR", err.Error())
					}
				}
			}

			var resp interface{}
			status := 200
			// 错误的返回
			if retErr != nil {
				errRsp := NewErrorResponse(retErr)
				switch opt.errorCodeFieldName {
				case ErrorCodeFieldNameMcode:
					errRsp.Mcode = retErr.Mcode()
				default:
					errRsp.Code = retErr.Mcode()
				}

				status = defaultErrorStatus
				if statusError, ok := retErr.(StatusError); ok {
					status = statusError.HttpStatus()
				}
				resp = errRsp

				httpCtx.Writer.Header().Set("X-Api-Code", resp.(*Response).Mcode)
				httpCtx.Writer.Header().Set("X-Api-Message", resp.(*Response).Message)
			} else {
				if _, ok := data.(NopResponse); !ok {
					resp = NewOkResponse(data)
				}
			}

			if resp != nil {
				httpCtx.JSON(status, resp)
			}

			if logMode > 0 {
				l := log.WithFields(logrus.Fields{
					"method":          httpCtx.Request.Method,
					"path":            httpCtx.Request.URL.Path,
					"delay":           time.Since(since),
					"query":           httpCtx.Request.URL.RawQuery,
					"pendingRequests": pendingRequests,
				})

				if retErr != nil && logMode&LogTypeError != 0 {
					l = l.WithFields(logrus.Fields{
						"mcode":   retErr.Mcode(),
						"message": retErr.Message(),
						"status":  status,
					})
					l.Error("HTTP request failed")
				} else if logMode&LogTypeSuccess != 0 {
					l.Info("HTTP request done")
				}
			}
		}()

		// 请求限制
		if maxPendingRequest > 0 {
			for err == nil {
				pending = atomic.LoadInt64(&pendingRequests)
				if pending+int64(requestWeight) > int64(maxPendingRequest) {
					err = ErrExceedMaxPendingRequest
					return
				}

				if !atomic.CompareAndSwapInt64(&pendingRequests, pending, pending+int64(requestWeight)) {
					continue
				}

				break
			}
		}

		if err != nil {
			return
		}

		defer atomic.AddInt64(&pendingRequests, int64(-requestWeight))

		data, err = f(httpCtx)
	}
}

func (wrapper *Wrapper) Handle(method string, srv HttpServer, path string, f WrappedFunc, options ...*WrapOption) {
	absPath := srv.(*gin.RouterGroup).BasePath() + path
	srv.Handle(method, path, wrapper.Wrap(f, absPath, options...))
}

func (wrapper *Wrapper) Get(srv HttpServer, path string, f WrappedFunc, options ...*WrapOption) {
	wrapper.Handle("GET", srv, path, f, options...)
}

func (wrapper *Wrapper) Any(srv HttpServer, path string, f WrappedFunc, options ...*WrapOption) {
	wrapper.Handle("GET", srv, path, f, options...)
	wrapper.Handle("POST", srv, path, f, options...)
	wrapper.Handle("PUT", srv, path, f, options...)
	wrapper.Handle("DELETE", srv, path, f, options...)
}

func (wrapper *Wrapper) Patch(srv HttpServer, path string, f WrappedFunc, options ...*WrapOption) {
	wrapper.Handle("PATCH", srv, path, f, options...)
}

func (wrapper *Wrapper) Post(srv HttpServer, path string, f WrappedFunc, options ...*WrapOption) {
	wrapper.Handle("POST", srv, path, f, options...)
}

func (wrapper *Wrapper) Put(srv HttpServer, path string, f WrappedFunc, options ...*WrapOption) {
	wrapper.Handle("PUT", srv, path, f, options...)
}

func (wrapper *Wrapper) Options(srv HttpServer, path string, f WrappedFunc, options ...*WrapOption) {
	wrapper.Handle("OPTIONS", srv, path, f, options...)
}

func (wrapper *Wrapper) Head(srv HttpServer, path string, f WrappedFunc, options ...*WrapOption) {
	wrapper.Handle("HEAD", srv, path, f, options...)
}

func (wrapper *Wrapper) Delete(srv HttpServer, path string, f WrappedFunc, options ...*WrapOption) {
	wrapper.Handle("DELETE", srv, path, f)
}

func (wrapper *Wrapper) LogNotProcess() func(ctx *gin.Context) {
	return func(ctx *gin.Context) {
		since := time.Now()
		ctx.Next()
		log := wrapper.cfg.log
		if ctx.Keys["easygin"] == nil && *wrapper.cfg.logMode&LogTypeError > 0 {
			l := log.WithFields(logrus.Fields{
				"method": ctx.Request.Method,
				"path":   ctx.Request.URL.Path,
				"delay":  time.Since(since),
				"query":  ctx.Request.URL.RawQuery,
				"status": ctx.Writer.Status(),
			})
			l.Error("HTTP request failed")
		}
	}
}
