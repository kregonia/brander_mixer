package status

import (
	"fmt"
	"testing"
	"time"
)

func TestGetWorkerStatus(t *testing.T) {
	fmt.Println(GetWorkerStatus())
	time.Sleep(5 * time.Second)
	fmt.Println(GetWorkerStatus())
}
