package server

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/HOFFMAN-ALTERNATIVE/distributed-rate-limiter/metrics"
	"github.com/HOFFMAN-ALTERNATIVE/distributed-rate-limiter/ratelimit"
	"github.com/prometheus/client_golang/prometheus"
)

func newTestServer(t *testing.T) *httptest.Server {
	t.Helper()
	reg := prometheus.NewRegistry()
	base, _ := ratelimit.NewMemoryTokenBucket(ratelimit.TokenBucket{Rate: 1, Burst: 2})
	s := &Server{Limiter: metrics.NewCollectors(reg).Instrument("test", base), Gatherer: reg}
	ts := httptest.NewServer(s.Handler())
	t.Cleanup(ts.Close)
	return ts
}

func get(t *testing.T, url string) (*http.Response, string) {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp, string(b)
}

func TestCheck(t *testing.T) {
	ts := newTestServer(t)
	for i, want := range []int{200, 200, 429} {
		resp, body := get(t, ts.URL+"/check?key=alice")
		if resp.StatusCode != want {
			t.Fatalf("request %d: status %d, want %d (%s)", i, resp.StatusCode, want, body)
		}
		var cr CheckResponse
		if err := json.Unmarshal([]byte(body), &cr); err != nil {
			t.Fatal(err)
		}
		if cr.Allowed != (want == 200) || cr.Key != "alice" || cr.Limit != 2 {
			t.Fatalf("unexpected body %+v", cr)
		}
		if want == 429 && (cr.RetryAfterMS <= 0 || resp.Header.Get("Retry-After") == "") {
			t.Fatalf("missing retry info: %+v headers=%v", cr, resp.Header)
		}
	}
	// cost is honoured and validated.
	if resp, _ := get(t, ts.URL+"/check?key=bob&cost=2"); resp.StatusCode != 200 {
		t.Fatalf("cost=2: %d", resp.StatusCode)
	}
	for _, q := range []string{"", "?key=x&cost=abc", "?key=x&cost=3", "?key=x&cost=0"} {
		if resp, body := get(t, ts.URL+"/check"+q); resp.StatusCode != 400 {
			t.Errorf("%q: status %d, want 400 (%s)", q, resp.StatusCode, body)
		}
	}
}

func TestCheckMethod(t *testing.T) {
	ts := newTestServer(t)
	req, _ := http.NewRequest(http.MethodDelete, ts.URL+"/check?key=a", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("status %d", resp.StatusCode)
	}
}

func TestOpsEndpoints(t *testing.T) {
	ts := newTestServer(t)
	get(t, ts.URL+"/check?key=a")
	for _, p := range []string{"/healthz", "/readyz", "/demo"} {
		if resp, _ := get(t, ts.URL+p); resp.StatusCode != 200 {
			t.Errorf("%s: %d", p, resp.StatusCode)
		}
	}
	_, body := get(t, ts.URL+"/metrics")
	if !strings.Contains(body, `ratelimit_decisions_total{decision="allowed",limiter="test"}`) {
		t.Fatalf("metrics missing decision counter:\n%s", body)
	}
}

func TestCheckRejectsOversizedKey(t *testing.T) {
	ts := newTestServer(t)
	long := strings.Repeat("k", MaxKeyBytes+1)
	if resp, _ := get(t, ts.URL+"/check?key="+long); resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("oversized key: got %d, want 400", resp.StatusCode)
	}
	if resp, _ := get(t, ts.URL+"/check?key="+long[:MaxKeyBytes]); resp.StatusCode != http.StatusOK {
		t.Fatalf("key at the limit: got %d, want 200", resp.StatusCode)
	}
}
