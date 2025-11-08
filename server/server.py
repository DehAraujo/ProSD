import zmq, json, os
from datetime import datetime

BROKER = "tcp://broker:5556"  # conecta no DEALER do broker (para REQ-REP)
PROXY_PUB = "tcp://proxy:5557"  # conecta no XSUB do proxy (para PUBLISH)
DATA_FILE = "/app/data/state.json"

os.makedirs("/app/data", exist_ok=True)

# inicializa storage
if not os.path.exists(DATA_FILE):
    with open(DATA_FILE, "w") as f:
        # "messages" é reservado para posts públicos, não armazenamos mensagens P2P aqui
        json.dump({"users": [], "channels": [], "messages": []}, f)

def load_state():
    try:
        with open(DATA_FILE, "r") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {"users": [], "channels": [], "messages": []}

def save_state(state):
    with open(DATA_FILE, "w") as f:
        json.dump(state, f, indent=2)

ctx = zmq.Context()

# Socket REP para REQ-REP (login, users, channel, channels, etc.)
rep = ctx.socket(zmq.REP)
rep.connect(BROKER)
print("Servidor (REP) conectado ao broker:", BROKER)

# Socket PUB para Publish-Subscribe (publicar mensagens em canais e P2P)
pub = ctx.socket(zmq.PUB)
pub.connect(PROXY_PUB)
print("Servidor (PUB) conectado ao proxy:", PROXY_PUB)

def now():
    return datetime.utcnow().isoformat()

# --- Helpers para Checagem Case-Insensitive ---

def check_user_exists(state, user_to_check):
    # Normaliza o nome de usuário fornecido
    user_key = user_to_check.lower()
    # Checa se a versão minúscula existe na lista de usuários (comparando com a versão minúscula dos usuários armazenados)
    return any(u["user"].lower() == user_key for u in state["users"])

def check_channel_exists(state, channel_to_check):
    # Canais geralmente são case-sensitive para o tópico, mas checamos existência
    return any(c["channel"] == channel_to_check for c in state["channels"])

def get_original_username(state, user_to_check):
    # Retorna o nome de usuário original (com case)
    user_key = user_to_check.lower()
    return next((u["user"] for u in state["users"] if u["user"].lower() == user_key), None)

# ---------------------------------------------

while True:
    try:
        raw = rep.recv_json()
    except Exception as e:
        print("Erro recv:", e)
        continue

    svc = raw.get("service")
    data = raw.get("data", {})
    state = load_state()

    # --- REQ-REP Services ---
    if svc == "login":
        user = data.get("user")
        ts = data.get("timestamp", now())
        
        # 🔹 Correção: Uso de check_user_exists para unicidade case-insensitive
        if not user:
            rep.send_json({"service":"login","data":{"status":"erro","timestamp":now(),"description":"user missing"}})
        elif check_user_exists(state, user):
            rep.send_json({"service":"login","data":{"status":"erro","timestamp":now(),"description":"user exists (case insensitive)"}})
        else:
            # Armazena o nome de usuário com o case original
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
        elif check_channel_exists(state, ch):
            rep.send_json({"service":"channel","data":{"status":"erro","timestamp":now(),"description":"already exists"}})
        else:
            state["channels"].append({"channel": ch, "timestamp": ts})
            save_state(state)
            rep.send_json({"service":"channel","data":{"status":"sucesso","timestamp":now()}})

    elif svc == "channels":
        rep.send_json({"service":"channels","data":{"timestamp":now(),"channels":[c["channel"] for c in state["channels"]]}})

    elif svc == "check_channel":
        ch = data.get("channel")
        if not ch:
            rep.send_json({"service":"check_channel","data":{"status":"erro","description":"missing channel"}})
        elif check_channel_exists(state, ch):
            rep.send_json({"service":"check_channel","data":{"status":"OK"}})
        else:
            rep.send_json({"service":"check_channel","data":{"status":"erro","description":f"channel '{ch}' not found"}})

    # --- P2P Message Service (MESSAGE) ---
    elif svc == "message":
        src = data.get("src")
        dst = data.get("dst")
        content = data.get("message")
        ts = data.get("timestamp", now())
        
        if not src or not dst or not content:
            rep.send_json({"service": "message", "data": {"status": "erro", "timestamp": now(), "description": "missing field"}})
            # 👇 CORREÇÃO: A linha 'continue' deve ter a mesma indentação que a linha 'rep.send_json' imediatamente acima.
            continue
        
        # 🔹 Checagem de existência case-insensitive para origem e destino
        src_exists = check_user_exists(state, src)
        dst_exists = check_user_exists(state, dst)

        if not src_exists:
            rep.send_json({"service": "message", "data": {"status": "erro", "timestamp": now(), "description": f"source user '{src}' not logged in"}})
        elif not dst_exists:
            rep.send_json({"service": "message", "data": {"status": "erro", "timestamp": now(), "description": f"destination user '{dst}' not found or logged out"}})
        else:
            # Encontra o nome de usuário de destino com o case original (o tópico é o nome do usuário)
            original_dst_user = get_original_username(state, dst)

            message_data = {"type": "p2p", "src": src, "content": content, "timestamp": ts}
            
            # Publica a mensagem no tópico do usuário de destino (que é o nome de usuário, ex: 'Joao')
            pub.send_multipart([original_dst_user.encode('utf-8'), json.dumps(message_data).encode('utf-8')])
            print(f"Mensagem privada enviada de {src} para {original_dst_user}")

            rep.send_json({"service": "message", "data": {"status": "sucesso", "timestamp": now()}})

    # --- PUBLISH Service ---
    elif svc == "publish":
        channel = data.get("channel")
        user = data.get("user")
        content = data.get("content")
        ts = data.get("timestamp", now())

        channel_exists = check_channel_exists(state, channel)
        # 🔹 Correção: Uso de check_user_exists para checar se o usuário está logado
        user_logged_in = check_user_exists(state, user)

        if not channel or not user or not content:
            rep.send_json({"service": "publish", "data": {"status": "erro", "timestamp": now(), "description": "missing field"}})
        elif not channel_exists:
            rep.send_json({"service": "publish", "data": {"status": "erro", "timestamp": now(), "description": f"channel '{channel}' not found"}})
        elif not user_logged_in:
            rep.send_json({"service": "publish", "data": {"status": "erro", "timestamp": now(), "description": f"user '{user}' not logged in"}})
        else:
            message_data = {"type": "publish", "channel": channel, "user": user, "content": content, "timestamp": ts}
            state["messages"].append(message_data)
            save_state(state)

            # O tópico é o nome do canal (channel)
            pub.send_multipart([channel.encode('utf-8'), json.dumps(message_data).encode('utf-8')])
            print(f"Mensagem publicada no canal {channel}: {content}")

            rep.send_json({"service": "publish", "data": {"status": "sucesso", "timestamp": now()}})

    else:
        rep.send_json({"service":"error","data":{"timestamp":now(),"description":"unknown service"}})