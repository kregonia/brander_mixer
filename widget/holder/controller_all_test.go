package holder

import (
	"testing"
	"time"

	worker_2_controller_service "github.com/kregonia/brander_mixer/script/rpc_server/worker"
)

func TestFlash(t *testing.T) {
	sh := NewStatusHolder()
	for i := 0; i < 3000; i++ {
		sh.AppendStatusByKey("test_01", &worker_2_controller_service.Status{CpuUsage: float32(i)})
	}
	time.Sleep(3 * time.Second)
}
