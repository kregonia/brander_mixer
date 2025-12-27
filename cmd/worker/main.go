package main

import (
	"context"
	"flag"
	"fmt"
	"time"

	"github.com/kregonia/brander_mixer/widget/connection"
)

var (
	password = flag.String("p", "", "the password of connect the center computer")
	target   = flag.String("t", "controller:50051", "the target address of center computer")
)

func main() {
	flag.Parse()
	ctx := context.Background()
	client := connection.InitWorkerConnection(*target)
	fmt.Println("⌛️ waiting for get machine ip...")
	ip := ""
	fmt.Println("⌛️ registing the machine to controller...")
	success := client.RegistWorker2Controller(ctx, ip, *password)
	if !success {
		panic("❌ failed to regist worker to controller")
	}
	defer client.Close()
	fmt.Println("✅ registing the machine successfully")
	fmt.Println("⌛️ sending hearting to controller...")
	go client.SendHearting(ctx, ip)
	time.Sleep(10 * time.Minute)
}
