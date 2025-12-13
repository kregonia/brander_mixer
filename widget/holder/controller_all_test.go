package holder

import (
	"testing"
	"time"

	controller_service "github.com/kregonia/brander_mixer/script/rpc_server/controller"
)

func TestFlash(t *testing.T) {
	sh := NewStatusHolder(10)
	for i := 0; i < 30; i++ {
		sh.AppendStatusByKey("test_01", &controller_service.Status{CpuUsage: float32(i)})
	}
	time.Sleep(3 * time.Second)
}
