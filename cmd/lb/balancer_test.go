package main

import (
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func testLessBusyServerSelector(t *testing.T, balancer *Balancer, expectedServer Server) {
	lessBusyServer, err := balancer.selectLessBusyServer()
	if err != nil {
		t.Fatalf("Failed to select the least busy server: %s", err)
	}
	assert.Equal(t, expectedServer, *lessBusyServer)
}

func TestLessBusyServerSelectorCase1(t *testing.T) {
	balancer := &Balancer{
		checker: Health{},
		availableServers: Servers{
			list: []*Server{
				{"server1:8080", 4},
				{"server2:8080", 8},
				{"server3:8080", 5},
			}},
	}
	expectedServer := Server{"server1:8080", 4}

	testLessBusyServerSelector(t, balancer, expectedServer)
}

func TestLessBusyServerSelectorCase2(t *testing.T) {
	balancer := &Balancer{
		checker: Health{},
		availableServers: Servers{
			list: []*Server{
				{"server1:8080", 23},
				{"server2:8080", 11},
				{"server3:8080", 15},
			}},
	}
	expectedServer := Server{"server2:8080", 11}

	testLessBusyServerSelector(t, balancer, expectedServer)
}

func TestLessBusyServerSelectorCase3(t *testing.T) {
	balancer := &Balancer{
		checker: Health{},
		availableServers: Servers{
			list: []*Server{
				{"server1:8080", 11},
				{"server2:8080", 11},
				{"server3:8080", 1},
			}},
	}
	expectedServer := Server{"server3:8080", 1}

	testLessBusyServerSelector(t, balancer, expectedServer)
}

func TestLessBusyServerSelectorCase4(t *testing.T) {
	balancer := &Balancer{
		checker: Health{},
		availableServers: Servers{
			list: []*Server{
				{"server1:8080", 0},
				{"server2:8080", 0},
				{"server3:8080", 0},
			}},
	}
	expectedServer := Server{"server1:8080", 0}

	testLessBusyServerSelector(t, balancer, expectedServer)
}

func TestLessBusyServerSelectorCase5(t *testing.T) {
	balancer := &Balancer{
		checker: Health{},
		availableServers: Servers{
			list: []*Server{}},
	}

	_, err := balancer.selectLessBusyServer()
	assert.NotNil(t, err)
	assert.Equal(t, "Servers are not available", err.Error())
}

type TestHealth struct {
	state map[string]bool
}

func (t TestHealth) health(dst string) bool {
	return t.state[dst]
}

func TestLessBusyServerSelectorCase6(t *testing.T) {
	balancer := &Balancer{
		checker: TestHealth{
			state: map[string]bool{
				"server1:8080": false,
				"server2:8080": true,
				"server3:8080": true,
			}},
		checkRate: 1,
		availableServers: Servers{
			list: []*Server{
				{"server1:8080", 4},
				{"server2:8080", 12},
				{"server3:8080", 28},
			}},
	}

	expectedServer := Server{"server2:8080", 12}

	assert.Equal(t, 3, len(balancer.availableServers.list), "before monitoring")
	balancer.MonitorServersState()
	time.Sleep(time.Duration(balancer.checkRate) * 2 * time.Second)
	assert.Equal(t, 2, len(balancer.availableServers.list), "after monitoring")

	testLessBusyServerSelector(t, balancer, expectedServer)

}
func TestLessBusyServerSelectorCase7(t *testing.T) {
	balancer := &Balancer{
		checker: TestHealth{
			state: map[string]bool{
				"server1:8080": false,
				"server2:8080": false,
				"server3:8080": true,
			}},
		checkRate: 1,
		availableServers: Servers{
			list: []*Server{
				{"server1:8080", 2},
				{"server2:8080", 4},
				{"server3:8080", 32},
			}},
	}

	expectedServer := Server{"server3:8080", 32}

	assert.Equal(t, 3, len(balancer.availableServers.list), "before monitoring")
	balancer.MonitorServersState()
	time.Sleep(time.Duration(balancer.checkRate) * 2 * time.Second)
	assert.Equal(t, 1, len(balancer.availableServers.list), "after monitoring")

	testLessBusyServerSelector(t, balancer, expectedServer)

}
func TestLessBusyServerSelectorCase8(t *testing.T) {
	balancer := &Balancer{
		checker: TestHealth{
			state: map[string]bool{
				"server1:8080": false,
				"server2:8080": false,
				"server3:8080": false,
			}},
		checkRate: 1,
		availableServers: Servers{
			list: []*Server{
				{"server1:8080", 2},
				{"server2:8080", 4},
				{"server3:8080", 32},
			}},
	}

	assert.Equal(t, 3, len(balancer.availableServers.list), "before monitoring")
	balancer.MonitorServersState()
	time.Sleep(time.Duration(balancer.checkRate) * 2 * time.Second)
	assert.Equal(t, 0, len(balancer.availableServers.list), "after monitoring")

	_, err := balancer.selectLessBusyServer()
	assert.NotNil(t, err)
	assert.Equal(t, "Servers are not available", err.Error())

}

func TestLessBusyServerSelectorCase9(t *testing.T) {
	balancer := &Balancer{
		checker: TestHealth{
			state: map[string]bool{
				"server1:8080": true,
				"server2:8080": true,
				"server3:8080": false,
			}},
		checkRate: 1,
		availableServers: Servers{
			list: []*Server{
				{"server1:8080", 5},
				{"server2:8080", 6},
				{"server3:8080", 2},
			}},
	}

	expectedServer := Server{"server3:8080", 2}

	assert.Equal(t, 3, len(balancer.availableServers.list), "before monitoring")
	balancer.MonitorServersState()
	time.Sleep(time.Duration(balancer.checkRate) * 2 * time.Second)
	assert.Equal(t, 2, len(balancer.availableServers.list), "during monitoring")

	balancer.checker = TestHealth{
		state: map[string]bool{
			"server1:8080": true,
			"server2:8080": true,
			"server3:8080": true,
		}}

	time.Sleep(time.Duration(balancer.checkRate) * 2 * time.Second)
	assert.Equal(t, 3, len(balancer.availableServers.list), "after monitoring")

	testLessBusyServerSelector(t, balancer, expectedServer)

}
