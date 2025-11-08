// client/client.js
const zmq = require("zeromq");
const readline = require("readline");

// Adicione uma variável para manter os canais subscritos
let subscribed_channels = [];
let username = null; // Variável para armazenar o nome de usuário logado

async function main() {
    const sock = new zmq.Request();
    await sock.connect("tcp://broker:5555");
    console.log("💬 Cliente conectado ao broker (tcp://broker:5555)");

    const sub_sock = new zmq.Subscriber();
    await sub_sock.connect("tcp://proxy:5558");
    console.log("📣 Cliente conectado ao proxy (tcp://proxy:5558) para receber mensagens");

    // Função para tratar as mensagens recebidas via SUB
    async function receiveMessages() {
        for await (const [topic, message] of sub_sock) {
            const channel = topic.toString();
            try {
                const msg = JSON.parse(message.toString());
                console.log(`\n[${channel}] ${msg.user}: ${msg.content}`);
                // Reexibe o prompt
                process.stdout.write("> ");
            } catch (e) {
                console.error("Erro ao parsear mensagem SUB:", e);
            }
        }
    }

    // Inicia o loop de recebimento de mensagens
    receiveMessages().catch(err => { console.error("Erro no loop SUB:", err); process.exit(1); });

    const rl = readline.createInterface({ input: process.stdin, output: process.stdout });
    let prompt = () => rl.question("> ", async (line) => {
        const parts = line.trim().split(" ");
        const cmd = parts[0] ? parts[0].toLowerCase() : "";
        
        // --- Comandos de REQ-REP (Parte 1 e 2) ---
        if (cmd === "exit" || cmd === "quit") {
            console.log("Saindo...");
            rl.close();
            process.exit(0);
        } else if (cmd === "login") {
            const user = parts[1];
            if (!user) { console.log("Uso: login <nome>"); return prompt(); }
            const msg = { service: "login", data: { user, timestamp: new Date().toISOString() } };
            await sock.send(JSON.stringify(msg));
            const [reply] = await sock.receive();
            const replyObj = JSON.parse(reply.toString());
            console.log("REPLY:", replyObj);
            if (replyObj.data.status === "sucesso") {
                username = user;
                console.log(`Usuário **${username}** logado com sucesso!`);
            }
        } else if (cmd === "users") {
            const msg = { service: "users", data: { timestamp: new Date().toISOString() } };
            await sock.send(JSON.stringify(msg));
            const [reply] = await sock.receive();
            console.log("REPLY:", JSON.parse(reply.toString()));
        } else if (cmd === "channel") {
            const ch = parts[1];
            if (!ch) { console.log("Uso: channel <nome>"); return prompt(); }
            const msg = { service: "channel", data: { channel: ch, timestamp: new Date().toISOString() } };
            await sock.send(JSON.stringify(msg));
            const [reply] = await sock.receive();
            console.log("REPLY:", JSON.parse(reply.toString()));
        } else if (cmd === "channels") {
            const msg = { service: "channels", data: { timestamp: new Date().toISOString() } };
            await sock.send(JSON.stringify(msg));
            const [reply] = await sock.receive();
            console.log("REPLY:", JSON.parse(reply.toString()));
        } else if (cmd === "subscribe") {
            const ch = parts[1];
            if (!ch) { console.log("Uso: subscribe <canal>"); return prompt(); }
            if (subscribed_channels.includes(ch)) { console.log(`Já está subscrito em ${ch}`); return prompt(); }

            // Inscreve o socket SUB no tópico do canal
            sub_sock.subscribe(ch);
            subscribed_channels.push(ch);
            console.log(`Inscrito no canal **${ch}**`);
        } else if (cmd === "post") {
            if (!username) { console.log("Erro: Faça login antes de postar."); return prompt(); }
            const ch = parts[1];
            const content = parts.slice(2).join(" ");
            if (!ch || !content) { console.log("Uso: post <canal> <mensagem>"); return prompt(); }

            const msg = { 
                service: "publish", 
                data: { 
                    channel: ch, 
                    user: username,
                    content: content, 
                    timestamp: new Date().toISOString() 
                } 
            };
            await sock.send(JSON.stringify(msg));
            const [reply] = await sock.receive();
            const replyObj = JSON.parse(reply.toString());
            console.log("REPLY:", replyObj);
        } else {
            console.log("Comandos: login <nome>, users, channel <nome>, channels, subscribe <canal>, post <canal> <msg>, exit");
        }
        prompt();
    });
    prompt();
}

main().catch(err => { console.error(err); process.exit(1); });