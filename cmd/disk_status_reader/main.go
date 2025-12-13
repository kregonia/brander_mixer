package main

import (
	"bufio"
	"encoding/binary"
	"flag"
	"fmt"
	"io"
	"os"

	"google.golang.org/protobuf/proto"

	controller_service "github.com/kregonia/brander_mixer/script/rpc_server/controller"
)

func main() {
	in := flag.String("in", "", "input .brander file")
	out := flag.String("out", "", "output .txt file")
	flag.Parse()

	if *in == "" || *out == "" {
		fmt.Println("usage: brander-decoder -in xxx.brander -out xxx.txt")
		os.Exit(1)
	}

	if err := decodeToTxt(*in, *out); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
}

func decodeToTxt(input, output string) error {
	inFile, err := os.Open(input)
	if err != nil {
		return err
	}
	defer inFile.Close()
	outFile, err := os.Create(output)
	if err != nil {
		return err
	}
	defer outFile.Close()

	reader := bufio.NewReader(inFile)
	writer := bufio.NewWriter(outFile)
	defer writer.Flush()

	var offset int64
	batchIdx := 0

	for {
		// 1️⃣ 读长度
		var lenBuf [4]byte
		_, err := io.ReadFull(reader, lenBuf[:])
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("read length failed: %w", err)
		}

		length := int64(binary.BigEndian.Uint32(lenBuf[:]))
		offset += 4

		// 2️⃣ 读数据
		data := make([]byte, length)
		_, err = io.ReadFull(reader, data)
		if err != nil {
			fmt.Fprintf(writer,
				"[WARN] truncated record at batch %d\n", batchIdx)
			break
		}

		offset += length

		// 3️⃣ protobuf 解码
		var rs controller_service.RepeatedStatus
		if err := proto.Unmarshal(data, &rs); err != nil {
			return fmt.Errorf("unmarshal failed at batch %d: %w", batchIdx, err)
		}

		// 4️⃣ 写 txt
		for i, st := range rs.Statuses {
			fmt.Fprintf(
				writer,
				"batch=%d status=%d %+v\n",
				batchIdx, i, st,
			)
		}

		batchIdx++
	}

	fmt.Printf("✔ decoded %d batches → %s\n", batchIdx, output)
	return nil
}
