// client/client.js
const zmq = require("zeromq");
const readline = require("readline");

async function main() {
  const sock = new zmq.Request();
  await sock.connect("tcp://broker:5555");
  console.log("💬 Cliente conectado ao broker (tcp://broker:5555)");

  const rl = readline.createInterface({ input: process.stdin, output: process.stdout });
  let prompt = () => rl.question("> ", async (line) => {
    const parts = line.trim().split(" ");
    const cmd = parts[0] ? parts[0].toLowerCase() : "";
    if (cmd === "exit" || cmd === "quit") {
      console.log("Saindo...");
      rl.close();
      process.exit(0);
    }
    if (cmd === "login") {
      const user = parts[1];
      if (!user) { console.log("Uso: login <nome>"); return prompt(); }
      const msg = { service: "login", data: { user, timestamp: new Date().toISOString() } };
      await sock.send(JSON.stringify(msg));
      const [reply] = await sock.receive();
      console.log("REPLY:", JSON.parse(reply.toString()));
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
    } else {
      console.log("Comandos: login <nome>, users, channel <nome>, channels, exit");
    }
    prompt();
  });
  prompt();
}

main().catch(err => { console.error(err); process.exit(1); });
