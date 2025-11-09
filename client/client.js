// client/client.js
const zmq = require("zeromq");
const readline = require("readline");

let subscribed_channels = [];
let username = null;

async function main() {
    // --- SOCKETS ---
    const sock = new zmq.Request();
    await sock.connect("tcp://broker:5555");
    console.log("💬 Cliente conectado ao broker (tcp://broker:5555)");

    const sub_sock = new zmq.Subscriber();
    await sub_sock.connect("tcp://proxy:5558");
    console.log("📣 Cliente conectado ao proxy (tcp://proxy:5558) para receber mensagens");

    // --- 🔥 INSCRIÇÃO AUTOMÁTICA NO CANAL GLOBAL ---
    const GLOBAL_CHANNEL = "canal_bot_go";
    sub_sock.subscribe(GLOBAL_CHANNEL);
    subscribed_channels.push(GLOBAL_CHANNEL);
    console.log(`🌐 Inscrito automaticamente em **${GLOBAL_CHANNEL}** para receber mensagens do bot`);

    // --- LOOP DE RECEBIMENTO ---
    async function receiveMessages() {
        for await (const [topic, message] of sub_sock) {
            const topicName = topic.toString();
            try {
                const msg = JSON.parse(message.toString());
                if (msg.type === "p2p") {
                    console.log(`\n📩 [PRIVADO DE ${msg.src}] ${msg.content}`);
                } else if (msg.type === "publish") {
                    console.log(`\n🌐 [${topicName}] ${msg.user}: ${msg.content}`);
                } else {
                    console.log(`\n📦 [${topicName}] ${message.toString()}`);
                }
            } catch {
                console.log(`\n⚠️ [${topicName}] Mensagem bruta: ${message.toString()}`);
            }
            process.stdout.write("> ");
        }
    }

    receiveMessages().catch(err => {
        console.error("Erro no loop SUB:", err);
        process.exit(1);
    });

    // --- INTERFACE CLI ---
    const rl = readline.createInterface({ input: process.stdin, output: process.stdout });

    const prompt = () => rl.question("> ", async (line) => {
        const parts = line.trim().split(" ");
        const cmd = parts[0]?.toLowerCase() || "";

        // === SAIR ===
        if (cmd === "exit" || cmd === "quit") {
            console.log("👋 Saindo...");
            rl.close();
            process.exit(0);
        }

        // === LOGIN ===
        else if (cmd === "login") {
            const user = parts[1];
            if (!user) {
                console.log("Uso: login <nome>");
                return prompt();
            }

            const msg = { service: "login", data: { user, timestamp: new Date().toISOString() } };
            await sock.send(JSON.stringify(msg));
            const [reply] = await sock.receive();
            const replyObj = JSON.parse(reply.toString());

            if (replyObj.data.status === "sucesso") {
                username = user;
                sub_sock.subscribe(username);
                subscribed_channels.push(username);
                console.log(`✅ Usuário **${username}** logado e inscrito no canal pessoal.`);
            } else {
                console.log(`⚠️ ${replyObj.data.description || "Falha no login."}`);
            }
        }

        // === OUTROS COMANDOS ===
        else if (cmd === "logged") {
            if (username)
                console.log(`👤 Logado como: ${username}`);
            else
                console.log("❌ Nenhum usuário logado.");
        }

        else if (cmd === "channels") {
            await sock.send(JSON.stringify({ service: "channels", data: {} }));
            const [reply] = await sock.receive();
            console.log("📡 Canais disponíveis:", JSON.parse(reply.toString()).data.channels.join(", "));
        }

        else if (cmd === "subscribe") {
            const ch = parts[1];
            if (!ch) {
                console.log("Uso: subscribe <canal>");
                return prompt();
            }
            if (subscribed_channels.includes(ch)) {
                console.log(`Já está inscrito em **${ch}**`);
                return prompt();
            }
            sub_sock.subscribe(ch);
            subscribed_channels.push(ch);
            console.log(`✅ Inscrito em **${ch}**`);
        }

        else if (cmd === "post") {
            const ch = parts[1];
            const content = parts.slice(2).join(" ");
            if (!username) return console.log("⚠️ Faça login antes.");
            if (!ch || !content) return console.log("Uso: post <canal> <mensagem>");

            const msg = {
                service: "publish",
                data: { channel: ch, user: username, content, timestamp: new Date().toISOString() }
            };
            await sock.send(JSON.stringify(msg));
            const [reply] = await sock.receive();
            const resp = JSON.parse(reply.toString());
            console.log(resp.data.status === "sucesso" ? `📤 Enviado para ${ch}` : `❌ ${resp.data.description}`);
        }

        else if (cmd === "mychannels") {
            console.log("📋 Canais:", subscribed_channels.join(", "));
        }

        else {
            console.log(`Comandos:
🟢 login <nome> | 📡 channels | 🔔 subscribe <canal> | 💬 post <canal> <msg>
📋 mychannels | ❌ exit
`);
        }

        prompt();
    });

    prompt();
}

main().catch(err => {
    console.error(err);
    process.exit(1);
});
