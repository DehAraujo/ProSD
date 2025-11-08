package main

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"time"

	zmq "github.com/pebbe/zmq4"
)

type Message struct {
	Service   string                 `json:"service"`
	Data      map[string]interface{} `json:"data"`
	Timestamp string                 `json:"timestamp"`
	Clock     int                    `json:"clock"`
}

func main() {
	rand.Seed(time.Now().UnixNano())
	clock := 0
	fmt.Println("🤖 Bot ativo e enviando mensagens a cada 10s")

	// DEALER para o broker
	cmd, _ := zmq.NewSocket(zmq.DEALER)
	cmd.Connect("tcp://broker:5555")

	// SUB para ouvir publicações
	sub, _ := zmq.NewSocket(zmq.SUB)
	sub.Connect("tcp://proxy:5558")
	sub.SetSubscribe("replicate")

	go func() {
		for {
			raw, err := sub.RecvMessageBytes(0)
			if err != nil {
				fmt.Println("Erro ao receber mensagem:", err)
				continue
			}
			if len(raw) > 0 {
				fmt.Println("📥 Recebido replicate:", string(raw[0]))
			}
		}
	}()
	
	for {
		time.Sleep(10 * time.Second)
		clock++
		msg := Message{
			Service:   "publish",
			Data:      map[string]interface{}{"user": "Bot_GO", "content": fmt.Sprintf("Ping %d", rand.Intn(1000))},
			Timestamp: time.Now().UTC().Format(time.RFC3339),
			Clock:     clock,
		}
		raw, _ := json.Marshal(msg)
		cmd.Send(string(raw), 0)
		fmt.Println("📤 Bot enviou:", msg.Data["content"])
	}
}
