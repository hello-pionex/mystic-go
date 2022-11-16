package invoke

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/hello-pionex/mystic-go/code"
)

func TestInvoke(t *testing.T) {
	parsePayload := func(userData interface{}, r *http.Response, err error) error {
		if err != nil {
			return err
		}

		b, err := ioutil.ReadAll(r.Body)
		if err != nil {
			return &InvokeError{Action: "ReadResponseBody", Err: fmt.Errorf("read response body:%v", err)}
		}

		type JsonWrapper struct {
			Mcode   string
			Result  bool
			Message string
			Data    json.RawMessage
		}

		var wrapper JsonWrapper
		if err := json.Unmarshal(b, &wrapper); err != nil {
			return &InvokeError{Action: "ParsePayload", Err: fmt.Errorf("parse json wrapper:%v", err)}
		}
		if !wrapper.Result {
			return code.NewMcode(wrapper.Mcode, wrapper.Message)
		}

		if err := json.Unmarshal(b, userData); err != nil {
			return &InvokeError{Action: "ParsePayload", Err: fmt.Errorf("parse json data:%v", err)}
		}

		return nil
	}

	data := map[string]interface{}{}

	client := &http.Client{
		Transport: &http.Transport{
			Proxy: http.ProxyFromEnvironment,
			DialContext: (&net.Dialer{
				Timeout:   30 * time.Second,
				KeepAlive: 30 * time.Second,
			}).DialContext,
			ForceAttemptHTTP2:     true,
			MaxIdleConns:          100,
			IdleConnTimeout:       90 * time.Second,
			TLSHandshakeTimeout:   10 * time.Second,
			ExpectContinueTimeout: 1 * time.Second,
		},
	}

	type TestResponse struct {
		Success bool `json:"success"`
		Result  []struct {
			ID          int       `json:"id"`
			Liquidation bool      `json:"liquidation"`
			Price       float64   `json:"price"`
			Side        string    `json:"side"`
			Size        float64   `json:"size"`
			Time        time.Time `json:"time"`
		} `json:"result"`
	}

	// template
	ftxApi := New().
		Timeout(time.Second*10).      // timeout
		EncodingGzip().               // easy way to set header Content-Encoding to be `gzip`
		OnlyStatus200().              // treat all status as error expect 200
		Header("Common-Header", "1"). // set user define header
		AutoCloseResponseBody().      // close response.Body in the end of request
		AfterRequest(parsePayload).   // callback after http.Do execute
		Client(client).               // user customized http.Client
		UserData(&data).              // data pass to AfterRequest
		Addr("ftx.com").              // target address
		BeforeRequest(func(req *http.Request) {
			fmt.Println("BeforeRequest")
		}).   // callback before http.Do execute, so user have a chance to customize request
		TLS() // change scheme from http to https

	ctx, cancelFn := context.WithDeadline(context.Background(), time.Now().Add(time.Second*5))
	defer cancelFn()

	var ret TestResponse
	rsp, err := ftxApi.Copy().
		Get("/api/markets/BTC/USDT/trades"). // method and path
		Timestamp().                         // easy way to set most use query timestamp=<millisecond of current time>
		Query("limit", 100).                 // query key&value
		Query("repeatQuery", 1).             // query key&value
		Query("repeatQuery", "2").           // query key&value
		Header("Request-Header", "1").       // set user define header
		Context(ctx).                        // context.Context of request
		Json(map[string]interface{}{
			"field1": 1,
			"field2": "2",
		}). // json payload
		AfterRequest(func(i interface{}, r *http.Response, err error) error {
			if err == nil {
				b, err := ioutil.ReadAll(r.Body)
				if err != nil {
					panic(err)
				}
				fmt.Println(string(b))
				json.Unmarshal(b, &ret)
			}
			return err
		}).
		Request() // execute the request
	if err != nil {
		_ = err
	}

	fmt.Println(ret)
	_ = rsp
}
