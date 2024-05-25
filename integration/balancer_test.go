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
// Проте якщо якийсь із серверів обробив замало чи забагато запитів, слід вважати, що балансувальник працює неправильно.
func TestBalancer(t *testing.T) {
	if _, exists := os.LookupEnv("INTEGRATION_TEST"); !exists {
		t.Skip("Integration test is not enabled")
	}

	clientsNum := 3
	requestsNum := 120
	rc := ResponseCounter{serverCts: make(map[string]int)}

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

	avgResponsesPerServer := requestsNum / len(rc.serverCts)
	for server, responsesNum := range rc.serverCts {
		t.Logf("server %s processed %d requests", server, responsesNum)
		ratio := float64(responsesNum) / float64(avgResponsesPerServer)
		assert.InDelta(t, 1, ratio, 0.5,
			"number of requests to server is outside permissible range",
		)
	}
}

type ResponseCounter struct {
	serverCts map[string]int
	mu        sync.Mutex
}

func (rc *ResponseCounter) Increment(server string) {
	rc.mu.Lock()
	defer rc.mu.Unlock()

	rc.serverCts[server]++
}

func BenchmarkBalancer(b *testing.B) {
	// TODO: Реалізуйте інтеграційний бенчмарк для балансувальникка.
}
