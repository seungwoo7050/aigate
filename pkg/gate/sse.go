package gate

import (
	"errors"
	"io"
	"net/http"
	"strings"

	"github.com/tidwall/gjson"
)

// IsSSERequest checks if an incoming HTTP request requests Server-Sent Events streaming,
// either via the Accept header or JSON body containing `"stream": true`.
func IsSSERequest(req *http.Request, body []byte) bool {
	if strings.Contains(strings.ToLower(req.Header.Get("Accept")), "text/event-stream") {
		return true
	}
	return len(body) > 0 && gjson.GetBytes(body, "stream").Bool()
}

// IsSSEResponse checks if an upstream HTTP response is a Server-Sent Events stream.
func IsSSEResponse(resp *http.Response) bool {
	if resp == nil {
		return false
	}
	return strings.Contains(strings.ToLower(resp.Header.Get("Content-Type")), "text/event-stream")
}

// ProxyStream streams Server-Sent Events from the upstream response to the downstream ResponseWriter
// with immediate chunk flushing via http.Flusher to minimize Time-To-First-Token (TTFT).
func ProxyStream(w http.ResponseWriter, resp *http.Response) error {
	defer resp.Body.Close()

	// Copy upstream response headers
	for k, vv := range resp.Header {
		for _, v := range vv {
			w.Header().Add(k, v)
		}
	}

	// Ensure essential streaming headers are present
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no")

	w.WriteHeader(resp.StatusCode)

	flusher, hasFlusher := w.(http.Flusher)
	if hasFlusher {
		flusher.Flush()
	}

	buf := make([]byte, 4096)
	for {
		n, err := resp.Body.Read(buf)
		if n > 0 {
			if _, writeErr := w.Write(buf[:n]); writeErr != nil {
				return writeErr
			}
			if hasFlusher {
				flusher.Flush()
			}
		}
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}
	}
}
