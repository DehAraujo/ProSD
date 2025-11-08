import zmq, json, os
from datetime import datetime

BROKER = "tcp://broker:5556"    # conecta no DEALER do broker (para REQ-REP)
PROXY_PUB = "tcp://proxy:5557"  # conecta no XSUB do proxy (para PUBLISH)
DATA_FILE = "/app/data/state.json"

os.makedirs("/app/data", exist_ok=True)
# inicializa storage
if not os.path.exists(DATA_FILE):
    # Adicionando a lista de 'messages' na inicialização
    with open(DATA_FILE, "w") as f:
        json.dump({"users": [], "channels": [], "messages": []}, f) 

def load_state():
    with open(DATA_FILE, "r") as f:
        return json.load(f)

def save_state(state):
    with open(DATA_FILE, "w") as f:
        json.dump(state, f, indent=2)

ctx = zmq.Context()

# Socket REP para REQ-REP (login, users, channel, channels)
rep = ctx.socket(zmq.REP)
rep.connect(BROKER)
print("Servidor (REP) conectado ao broker:", BROKER)

# Novo Socket PUB para Publish-Subscribe (publicar mensagens)
pub = ctx.socket(zmq.PUB)
pub.connect(PROXY_PUB)
print("Servidor (PUB) conectado ao proxy:", PROXY_PUB)

def now():
    return datetime.utcnow().isoformat()

while True:
    try:
        # Recebe a requisição via REP (do broker)
        raw = rep.recv_json()
    except Exception as e:
        print("Erro recv:", e)
        continue

    svc = raw.get("service")
    data = raw.get("data", {})
    # Carrega estado atual
    state = load_state()

    # --- Lógica da Parte 1 (REQ-REP) ---

    if svc == "login":
        user = data.get("user")
        ts = data.get("timestamp", now())
        # checa existência
        exists = any(u["user"] == user for u in state["users"])
        if not user:
            rep.send_json({"service":"login","data":{"status":"erro","timestamp":now(),"description":"user missing"}})
        elif exists:
            rep.send_json({"service":"login","data":{"status":"erro","timestamp":now(),"description":"user exists"}})
        else:
            state["users"].append({"user": user, "timestamp": ts})
            save_state(state)
            rep.send_json({"service":"login","data":{"status":"sucesso","timestamp":now()}})

    elif svc == "users":
        rep.send_json({"service":"users","data":{"timestamp":now(),"users":[u["user"] for u in state["users"]]}})

    elif svc == "channel":
        ch = data.get("channel")
        ts = data.get("timestamp", now())
        if not ch:
            rep.send_json({"service":"channel","data":{"status":"erro","timestamp":now(),"description":"channel missing"}})
        elif any(c["channel"] == ch for c in state["channels"]):
            rep.send_json({"service":"channel","data":{"status":"erro","timestamp":now(),"description":"already exists"}})
        else:
            state["channels"].append({"channel": ch, "timestamp": ts})
            save_state(state)
            rep.send_json({"service":"channel","data":{"status":"sucesso","timestamp":now()}})

    elif svc == "channels":
        # Corrigido: 'users' para 'channels' na resposta
        rep.send_json({"service":"channels","data":{"timestamp":now(),"channels":[c["channel"] for c in state["channels"]]}})

    # --- Lógica da Parte 2 (PUBLISH) ---
    elif svc == "publish":
        channel = data.get("channel")
        user = data.get("user")
        content = data.get("content")
        ts = data.get("timestamp", now())

        # 1. Validação
        channel_exists = any(c["channel"] == channel for c in state["channels"])
        user_exists = any(u["user"] == user for u in state["users"])

        if not channel or not user or not content:
            rep.send_json({"service": "publish", "data": {"status": "erro", "timestamp": now(), "description": "missing field"}})
        elif not channel_exists:
            rep.send_json({"service": "publish", "data": {"status": "erro", "timestamp": now(), "description": f"channel '{channel}' not found"}})
        elif not user_exists:
            rep.send_json({"service": "publish", "data": {"status": "erro", "timestamp": now(), "description": f"user '{user}' not logged in"}})
        else:
            # 2. Persistência
            message_data = {"channel": channel, "user": user, "content": content, "timestamp": ts}
            state["messages"].append(message_data)
            save_state(state)
            
            # 3. Publicação (topic é o nome do canal)
            pub.send_multipart([channel.encode('utf-8'), json.dumps(message_data).encode('utf-8')])
            print(f"Mensagem publicada no canal {channel}: {content}")

            # 4. Resposta ao cliente REQ
            rep.send_json({"service": "publish", "data": {"status": "sucesso", "timestamp": now()}})

    else:
        rep.send_json({"service":"error","data":{"timestamp":now(),"description":"unknown service"}})