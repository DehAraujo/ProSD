// client/client.js

const zmq = require("zeromq");
const readline = require("readline");

let subscribed_channels = [];
let username = null;

async function main() {
    const sock = new zmq.Request();
    await sock.connect("tcp://broker:5555");
    console.log("💬 Cliente conectado ao broker (tcp://broker:5555)");

    const sub_sock = new zmq.Subscriber();
    await sub_sock.connect("tcp://proxy:5558");
    console.log("📣 Cliente conectado ao proxy (tcp://proxy:5558) para receber mensagens");

    // --- Loop de recebimento de mensagens (SUB) ---
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
                    console.log(`\n📦 [${topicName}] Mensagem: ${message.toString()}`);
                }
                process.stdout.write("> ");
            } catch {
                console.log(`\n⚠️ [${topicName}] Mensagem bruta: ${message.toString()}`);
                process.stdout.write("> ");
            }
        }
    }
    receiveMessages().catch(err => { console.error("Erro no loop SUB:", err); process.exit(1); });

    // --- Interface CLI ---
    const rl = readline.createInterface({ input: process.stdin, output: process.stdout });

    const prompt = () => rl.question("> ", async (line) => {
        const parts = line.trim().split(" ");
        const cmd = parts[0]?.toLowerCase() || "";

        // --- SAIR ---
        if (cmd === "exit" || cmd === "quit") {
            console.log("👋 Saindo...");
            rl.close();
            process.exit(0);
        }

        // --- LOGIN ---
        else if (cmd === "login") {
            const user = parts[1];
            if (!user) { console.log("Uso: login <nome>"); return prompt(); }

            // Tenta logar
            const msg = { service: "login", data: { user, timestamp: new Date().toISOString() } };
            await sock.send(JSON.stringify(msg));
            const [reply] = await sock.receive();
            const replyObj = JSON.parse(reply.toString());

            if (replyObj.data.status === "sucesso") {
                username = user;
                sub_sock.subscribe(username);
                subscribed_channels.push(username);
                console.log(`✅ Usuário **${username}** logado com sucesso!`);
            } 
            else if (replyObj.data.status === "ja_logado") {
                username = user;
                console.log(`✅ Usuário **${username}** já estava logado.`);
            } 
            else if (replyObj.data.status === "erro" && replyObj.data.description.includes("exists")) {
                username = user;
                console.log(`⚠️ Usuário **${username}** já existe. Logado como ele mesmo.`);
            }
            else {
                console.log("❌ Erro ao logar:", replyObj.data.description);
            }
        }

        // --- MOSTRAR LOGIN ATUAL ---
        else if (cmd === "logged") {
            if (username) console.log(`👤 Logado como: ${username}`);
            else console.log("❌ Nenhum usuário logado.");
        }

        // --- LISTAR USUÁRIOS ---
        else if (cmd === "users") {
            await sock.send(JSON.stringify({ service: "users", data: { timestamp: new Date().toISOString() } }));
            const [reply] = await sock.receive();
            console.log("👥 Usuários:", JSON.parse(reply.toString()).data.users.join(", "));
        }

        // --- LISTAR CANAIS ---
        else if (cmd === "channels") {
            await sock.send(JSON.stringify({ service: "channels", data: { timestamp: new Date().toISOString() } }));
            const [reply] = await sock.receive();
            console.log("📡 Canais disponíveis:", JSON.parse(reply.toString()).data.channels.join(", "));
        }

        // --- CRIAR CANAL ---
        else if (cmd === "channel") {
            const ch = parts[1];
            if (!ch) { console.log("Uso: channel <nome>"); return prompt(); }

            const msg = { service: "channel", data: { channel: ch, timestamp: new Date().toISOString() } };
            await sock.send(JSON.stringify(msg));
            const [reply] = await sock.receive();
            const obj = JSON.parse(reply.toString());
            if (obj.data.status === "sucesso")
                console.log(`✅ Canal **${ch}** criado com sucesso.`);
            else
                console.log(`❌ Erro ao criar canal: ${obj.data.description}`);
        }

        // --- SUBSCRIBE --- ✅ Versão corrigida
        else if (cmd === "subscribe") {
            const ch = parts[1];
            if (!ch) { 
                console.log("Uso: subscribe <canal>"); 
                return prompt(); 
            }
            if (!username) { 
                console.log("Erro: Faça login antes de se inscrever."); 
                return prompt(); 
            }
            if (subscribed_channels.includes(ch)) { 
                console.log(`Já está inscrito em **${ch}**`); 
                return prompt(); 
            }

            // 🔍 1. Verifica com o servidor se o canal existe
            const checkMsg = { service: "check_channel", data: { channel: ch } };
            await sock.send(JSON.stringify(checkMsg));
            const [checkReply] = await sock.receive();
            const checkObj = JSON.parse(checkReply.toString());

            if (checkObj.data.status !== "OK") {
                console.log(`❌ Canal '${ch}' não existe. Crie-o antes de se inscrever.`);
                return prompt();
            }

            // ✅ 2. Solicita ao servidor o subscribe (apenas para registro)
            const subMsg = { service: "subscribe", data: { channel: ch, user: username } };
            await sock.send(JSON.stringify(subMsg));
            const [subReply] = await sock.receive();
            const subObj = JSON.parse(subReply.toString());

            if (subObj.data.status === "sucesso") {
                sub_sock.subscribe(ch);
                subscribed_channels.push(ch);
                console.log(`✅ Inscrito com sucesso no canal **${ch}**`);
            } else {
                console.log(`❌ Erro: ${subObj.data.description}`);
            }
        }

        // --- DESINSCRER-SE ---
        else if (cmd === "unsubscribe") {
            const ch = parts[1];
            if (!ch) { console.log("Uso: unsubscribe <canal>"); return prompt(); }
            if (!username) { console.log("Erro: Faça login antes de se desinscrever."); return prompt(); }
            if (!subscribed_channels.includes(ch)) {
                console.log(`Você não está inscrito em **${ch}**`);
                return prompt();
            }

            sub_sock.unsubscribe(ch);
            subscribed_channels = subscribed_channels.filter(c => c !== ch);
            console.log(`🚪 Saiu do canal **${ch}** com sucesso.`);
        }

        // --- POST ---
        else if (cmd === "publisher") {
            if (!username) { console.log("Erro: Faça login antes de postar."); return prompt(); }
            const ch = parts[1];
            const content = parts.slice(2).join(" ");
            if (!ch || !content) { console.log("Uso: post <canal> <mensagem>"); return prompt(); }

            const msg = { 
                service: "publish", 
                data: { 
                    channel: ch, 
                    user: username, 
                    content, 
                    timestamp: new Date().toISOString() 
                } 
            };
            await sock.send(JSON.stringify(msg));
            const [reply] = await sock.receive();
            const resp = JSON.parse(reply.toString());
            if (resp.data.status === "sucesso")
                console.log(`📤 Mensagem enviada para ${ch}`);
            else
                console.log(`❌ Erro ao enviar: ${resp.data.description}`);
        }

        // --- MENSAGEM PRIVADA ---
        else if (cmd === "msg") {
            if (!username) { console.log("Erro: Faça login antes de enviar mensagens."); return prompt(); }
            const dst = parts[1];
            const content = parts.slice(2).join(" ");
            if (!dst || !content) { console.log("Uso: msg <destinatario> <mensagem>"); return prompt(); }

            const msg = { 
                service: "message", 
                data: { src: username, dst, message: content, timestamp: new Date().toISOString() } 
            };
            await sock.send(JSON.stringify(msg));
            const [reply] = await sock.receive();
            console.log("REPLY:", JSON.parse(reply.toString()));
        }

        // --- MEUS CANAIS ---
        else if (cmd === "mychannels") {
            if (subscribed_channels.length === 0)
                console.log("Você não está inscrita em nenhum canal.");
            else
                console.log("📡 Canais inscritos:", subscribed_channels.join(", "));
        }

        // --- AJUDA ---
        else {
            console.log(`
Comandos disponíveis:
🟢 login <nome>          → Fazer login
👤 logged                → Mostrar login atual
👥 users                 → Listar usuários logados
📡 channels              → Listar canais disponíveis
➕ channel <nome>        → Criar um novo canal
🔔 subscribe <canal>     → Entrar em um canal existente
🚪 unsubscribe <canal>   → Sair de um canal
💬 post <canal> <msg>    → Enviar mensagem para um canal
📨 msg <dest> <msg>      → Enviar mensagem privada
📋 mychannels            → Ver canais onde está inscrita
❌ exit / quit           → Sair
`);
        }

        prompt();
    });

    prompt();
}

main().catch(err => { console.error(err); process.exit(1); });
