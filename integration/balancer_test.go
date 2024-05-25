package integration

import (
	"fmt"
	"net/http"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

const baseAddress = "http://balancer:8090"

var client = http.Client{
	Timeout: 3 * time.Second,
}

// Балансувальник з алгоритмом на найменшу кількість з'єднань має розподілити запити між усіма доступними серверами.
// За статистикою, перший сервер зі списку доступних оброблює найбільше запитів, другий - трохи менше і так далі.
// Проте якщо якийсь із серверів обробив замало чи забагато запитів, слід вважати, що балансувальник працює неправильно.
func TestBalancer(t *testing.T) {
	if _, exists := os.LookupEnv("INTEGRATION_TEST"); !exists {
		t.Skip("Integration test is not enabled")
	}

	clientsNum := 3
	requestsNum := 120
	rc := ResponseCounter{
		cts: map[string]struct {
			actual, expected int
		}{
			"server1:8080": {expected: 50},
			"server2:8080": {expected: 40},
			"server3:8080": {expected: 30},
		},
	}

	wg := sync.WaitGroup{}
	for i := 0; i < clientsNum; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()

			for j := 0; j < requestsNum/clientsNum; j++ {
				url := fmt.Sprintf("%s/api/v1/some-data", baseAddress)
				resp, err := client.Get(url)
				assert.NoError(t, err)
				assert.Equal(t, http.StatusOK, resp.StatusCode,
					"bad response status",
				)
				server := resp.Header.Get("lb-from")
				assert.NotEmpty(t, server,
					"no 'lb-from' header in response",
				)
				rc.Increment(server)
				resp.Body.Close()
			}
		}()
	}
	wg.Wait()

	for server, ct := range rc.cts {
		t.Logf("server %s processed %d requests", server, ct.actual)
		assert.InDelta(t, ct.expected, ct.actual, 20,
			"number of requests to server is outside permissible range",
		)
	}
}

type ResponseCounter struct {
	cts map[string]struct {
		actual, expected int
	}
	mu sync.Mutex
}

func (rc *ResponseCounter) Increment(server string) {
	rc.mu.Lock()
	defer rc.mu.Unlock()

	ct := rc.cts[server]
	ct.actual++
	rc.cts[server] = ct
}

func BenchmarkBalancer(b *testing.B) {
	if _, exists := os.LookupEnv("INTEGRATION_TEST"); !exists {
		b.Skip("Integration benchmark is not enabled")
	}

	for i := 0; i < b.N; i++ {
		url := fmt.Sprintf("%s/api/v1/some-data", baseAddress)
		resp, err := client.Get(url)
		assert.NoError(b, err)
		assert.Equal(b, http.StatusOK, resp.StatusCode,
			"bad response status",
		)
		resp.Body.Close()
	}
}
