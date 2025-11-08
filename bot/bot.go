package main

import (
    "encoding/json"
    "fmt"
    "math/rand"
    "time"

    zmq "github.com/pebbe/zmq4"
)

// O Bot agora também precisa de um canal para publicar/assinar
const CHANNEL = "canal_bot_go"

// Estrutura de mensagens
type Message struct {
    Service     string                 `json:"service"`
    Data        map[string]interface{} `json:"data"`
    Timestamp   string                 `json:"timestamp"`
    Clock       int                    `json:"clock"`
}

func main() {
    rand.Seed(time.Now().UnixNano())
    clock := 0
    fmt.Println("🤖 Bot ativo e enviando mensagens a cada 10s")

    // DEALER para o broker (envia REQ com service:publish)
    cmd, _ := zmq.NewSocket(zmq.DEALER)
    cmd.Connect("tcp://broker:5555")

    // SUB para ouvir publicações (agora no canal específico)
    sub, _ := zmq.NewSocket(zmq.SUB)
    sub.Connect("tcp://proxy:5558")
    // O bot se inscreve no canal que ele e outros usarão
    sub.SetSubscribe(CHANNEL)
    fmt.Printf("📣 Bot inscrito no tópico: %s\n", CHANNEL)


    // 1. **Login Inicial** (para que o servidor reconheça o usuário "Bot_GO")
    loginMsg := Message{
        Service:    "login",
        Data:       map[string]interface{}{"user": "Bot_GO", "timestamp": time.Now().UTC().Format(time.RFC3339)},
        Timestamp:  time.Now().UTC().Format(time.RFC3339),
        Clock:      0,
    }
    rawLogin, _ := json.Marshal(loginMsg)
    cmd.Send(string(rawLogin), 0)
    // DEALER precisa de uma REP, mesmo que não a usemos para nada neste ponto
    if _, err := cmd.RecvMessageBytes(0); err != nil {
        fmt.Println("Erro ao receber REP do login:", err)
    } else {
        fmt.Println("✅ Bot_GO logado no servidor.")
    }

    // 2. **Loop de Recebimento de Mensagens (SUB)**
    go func() {
        for {
            // Espera por mensagem multipart [topic, message_json]
            raw, err := sub.RecvMessageBytes(0)
            if err != nil {
                fmt.Println("Erro ao receber mensagem SUB:", err)
                continue
            }
            if len(raw) == 2 {
                topic := string(raw[0])
                messageJSON := string(raw[1])
                // Parse do JSON para extrair o conteúdo
                var receivedMsg map[string]interface{}
                if err := json.Unmarshal([]byte(messageJSON), &receivedMsg); err == nil {
                    // Acessa os dados da mensagem
                    user := receivedMsg["user"]
                    content := receivedMsg["content"]
                    // Apenas exibe o conteúdo
                    fmt.Printf("📥 Recebido [%s] de %s: %s\n", topic, user, content)
                }
            }
        }
    }()
    
    // 3. **Loop de Envio de Mensagens (DEALER/PUBLISH)**
    for {
        time.Sleep(10 * time.Second)
        clock++
        pingContent := fmt.Sprintf("Ping %d", rand.Intn(1000))
        
        // Mensagem agora segue o formato "publish"
        msg := Message{
            Service:    "publish",
            Data:       map[string]interface{}{
                "channel": CHANNEL, 
                "user": "Bot_GO", 
                "content": pingContent,
            },
            Timestamp: time.Now().UTC().Format(time.RFC3339),
            Clock:     clock,
        }
        raw, _ := json.Marshal(msg)
        cmd.Send(string(raw), 0)
        fmt.Println("📤 Bot enviou:", pingContent)

        // Bot deve esperar a REP do servidor
        if _, err := cmd.RecvMessageBytes(0); err != nil {
            fmt.Println("Erro ao receber REP do publish:", err)
        }
    }
}