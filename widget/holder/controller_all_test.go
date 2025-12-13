package holder

import (
	"fmt"
	"testing"

	controller_service "github.com/kregonia/brander_mixer/script/rpc_server/controller"
)

func TestFlash(t *testing.T) {
	sh := NewStatusHolder(10)
	for i := 0; i < 30; i++ {
		if i >= 11 {
			sh.AppendStatusByKey("test_01", &controller_service.Status{CpuUsage: float32(i)})
		} else {
			sh.AppendStatusByKey("test_02", &controller_service.Status{CpuUsage: float32(i)})
			t2, ok := sh.Load("test_02")
			if !ok {
				t.Fatal("load test_02 failed")
			}
			fmt.Println(t2.refreshTimes)
		}
	}

}
