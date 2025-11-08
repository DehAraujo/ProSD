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

    // --- RECEBIMENTO DE MENSAGENS ---
    async function receiveMessages() {
        for await (const [topic, message] of sub_sock) {
            const topicName = topic.toString();
            try {
                const msg = JSON.parse(message.toString());
                if (msg.type === "p2p") {
                    console.log(`\n[💌 PRIVADO DE ${msg.src}] ${msg.content}`);
                } else if (msg.type === "publish") {
                    console.log(`\n[📢 ${topicName}] ${msg.user}: ${msg.content}`);
                } else {
                    console.log(`\n[RECEBIDO ${topicName}] ${message.toString()}`);
                }
            } catch {
                console.log(`\n[RECEBIDO ${topicName}] ${message.toString()}`);
            }
            process.stdout.write("> ");
        }
    }
    receiveMessages().catch(err => console.error("Erro no loop SUB:", err));

    // --- INTERFACE CLI ---
    const rl = readline.createInterface({ input: process.stdin, output: process.stdout });

    async function handleCommand(line) {
        const parts = line.trim().split(" ");
        const cmd = parts[0]?.toLowerCase() || "";

        // === COMANDOS ===
        if (cmd === "exit" || cmd === "quit") {
            console.log("👋 Saindo...");
            rl.close();
            process.exit(0);
        }

        // --- LOGIN ---
        else if (cmd === "login") {
            const user = parts[1];
            if (!user) {
                console.log("Uso: login <nome>");
                return;
            }

            // Envia tentativa de login
            const msg = { service: "login", data: { user, timestamp: new Date().toISOString() } };
            await sock.send(JSON.stringify(msg));
            const [reply] = await sock.receive();
            const replyObj = JSON.parse(reply.toString());

            // --- Tratamento do login ---
            if (replyObj.data.status === "sucesso") {
                username = user;
                if (!subscribed_channels.includes(username)) {
                    sub_sock.subscribe(username);
                    subscribed_channels.push(username);
                }
                console.log(`✅ Logado como **${username}** com sucesso!`);
            } else if (replyObj.data.status === "ja_logado") {
                username = user;
                console.log(`⚠️ Usuário **${username}** já estava logado. Reconectado.`);
                if (!subscribed_channels.includes(username)) {
                    sub_sock.subscribe(username);
                    subscribed_channels.push(username);
                }
            } else {
                console.log(`❌ Erro no login: ${replyObj.data.description}`);
            }
        }

        // --- VER LOGIN ATUAL ---
        else if (cmd === "whoami") {
            if (username) console.log(`👤 Usuário atual: **${username}**`);
            else console.log("⚠️ Nenhum usuário logado.");
        }

        // --- LISTAR CANAIS DISPONÍVEIS ---
        else if (cmd === "channels") {
            const msg = { service: "channels", data: { timestamp: new Date().toISOString() } };
            await sock.send(JSON.stringify(msg));
            const [reply] = await sock.receive();
            const res = JSON.parse(reply.toString());
            console.log("📜 Canais disponíveis:", res.data.channels || res);
        }

        // --- LISTAR CANAIS INSCRITOS ---
        else if (cmd === "mychannels") {
            if (subscribed_channels.length === 0) console.log("❕ Você não está inscrito em nenhum canal.");
            else console.log("📦 Canais inscritos:", subscribed_channels.join(", "));
        }

        // --- INFORMAÇÕES DE UM CANAL ---
        else if (cmd === "channel") {
            const ch = parts[1];
            if (!ch) { console.log("Uso: channel <nome>"); return; }
            const msg = { service: "channel", data: { channel: ch, timestamp: new Date().toISOString() } };
            await sock.send(JSON.stringify(msg));
            const [reply] = await sock.receive();
            console.log("📋 Info do canal:", JSON.parse(reply.toString()));
        }

        // --- INSCRIÇÃO EM CANAL ---
        else if (cmd === "subscribe") {
            const ch = parts[1];
            if (!ch) { console.log("Uso: subscribe <canal>"); return; }
            if (!username) { console.log("⚠️ Faça login antes de se inscrever."); return; }
            if (subscribed_channels.includes(ch)) { console.log(`Já está inscrito em **${ch}**`); return; }

            // Verifica se canal existe
            await sock.send(JSON.stringify({ service: "check_channel", data: { channel: ch } }));
            const [checkReply] = await sock.receive();
            const checkObj = JSON.parse(checkReply.toString());

            if (checkObj.data.status !== "OK") {
                console.log(`❌ Canal **${ch}** não existe.`);
                return;
            }

            // Solicita inscrição
            await sock.send(JSON.stringify({ service: "subscribe", data: { channel: ch, user: username } }));
            const [subReply] = await sock.receive();
            const subObj = JSON.parse(subReply.toString());

            if (subObj.data.status === "sucesso") {
                sub_sock.subscribe(ch);
                subscribed_channels.push(ch);
                console.log(`✅ Inscrito com sucesso no canal **${ch}**`);
            } else {
                console.log(`Erro ao inscrever: ${subObj.data.description}`);
            }
        }

        // --- SAIR DO CANAL ---
        else if (cmd === "unsubscribe") {
            const ch = parts[1];
            if (!ch) { console.log("Uso: unsubscribe <canal>"); return; }
            if (!username) { console.log("⚠️ Faça login antes de sair de canais."); return; }
            if (!subscribed_channels.includes(ch)) {
                console.log(`Você não está inscrito em **${ch}**`);
                return;
            }

            await sock.send(JSON.stringify({ service: "unsubscribe", data: { channel: ch, user: username } }));
            const [reply] = await sock.receive();
            const replyObj = JSON.parse(reply.toString());
            if (replyObj.data.status === "sucesso") {
                sub_sock.unsubscribe(ch);
                subscribed_channels = subscribed_channels.filter(c => c !== ch);
                console.log(`🚪 Saiu do canal **${ch}** com sucesso.`);
            } else {
                console.log(`Erro ao sair: ${replyObj.data.description}`);
            }
        }

        // --- PUBLICAR MENSAGEM ---
        else if (cmd === "post") {
            if (!username) { console.log("⚠️ Faça login antes de postar."); return; }
            const ch = parts[1];
            const content = parts.slice(2).join(" ");
            if (!ch || !content) { console.log("Uso: post <canal> <mensagem>"); return; }

            const msg = { service: "publish", data: { channel: ch, user: username, content, timestamp: new Date().toISOString() } };
            await sock.send(JSON.stringify(msg));
            const [reply] = await sock.receive();
            console.log("📨 Servidor:", JSON.parse(reply.toString()));
        }

        // --- ENVIAR MENSAGEM PRIVADA ---
        else if (cmd === "msg") {
            if (!username) { console.log("⚠️ Faça login antes de enviar mensagens."); return; }
            const dst = parts[1];
            const content = parts.slice(2).join(" ");
            if (!dst || !content) { console.log("Uso: msg <destinatario> <mensagem>"); return; }

            const msg = { service: "message", data: { src: username, dst, message: content, timestamp: new Date().toISOString() } };
            await sock.send(JSON.stringify(msg));
            const [reply] = await sock.receive();
            console.log("📨 Servidor:", JSON.parse(reply.toString()));
        }

        else {
            console.log(`
🧭 Comandos disponíveis:
  login <nome>         → Fazer login
  whoami               → Mostrar usuário logado
  channels             → Listar canais disponíveis
  mychannels           → Mostrar canais inscritos
  channel <nome>       → Info de um canal
  subscribe <canal>    → Entrar em um canal existente
  unsubscribe <canal>  → Sair de um canal
  post <canal> <msg>   → Enviar mensagem pública
  msg <user> <msg>     → Enviar mensagem privada
  exit                 → Sair
            `);
        }
    }

    // --- LOOP CLI ---
    rl.on("line", async (line) => {
        try { await handleCommand(line); }
        catch (err) { console.error("❌ Erro:", err); }
        process.stdout.write("> ");
    });

    process.stdout.write("> ");
}

main().catch(err => { console.error(err); process.exit(1); });
