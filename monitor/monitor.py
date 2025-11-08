from flask import Flask, Response
import zmq, threading, json

app = Flask(__name__)
ctx = zmq.Context()
sub = ctx.socket(zmq.SUB)
sub.connect("tcp://proxy:5558")
sub.setsockopt_string(zmq.SUBSCRIBE, "")

messages = []

def listen():
    while True:
        topic, msg = sub.recv_multipart()
        data = f"[{topic.decode()}] {msg.decode()}"
        messages.append(data)
        if len(messages) > 100:
            messages.pop(0)

threading.Thread(target=listen, daemon=True).start()

@app.route("/")
def index():
    return """<!doctype html>
<html>
  <body>
    <h2>🧭 Monitor de Mensagens ZMQ</h2>
    <pre id="log"></pre>
    <script>
      const log = document.getElementById('log');
      async function update() {
        const res = await fetch('/stream');
        const reader = res.body.getReader();
        const decoder = new TextDecoder();
        while (true) {
          const {value, done} = await reader.read();
          if (done) break;
          log.textContent += decoder.decode(value);
          log.scrollTop = log.scrollHeight;
        }
      }
      update();
    </script>
  </body>
</html>"""

@app.route("/stream")
def stream():
    def event_stream():
        idx = 0
        while True:
            if len(messages) > idx:
                yield messages[idx] + "\n"
                idx += 1
    return Response(event_stream(), mimetype="text/plain")

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
