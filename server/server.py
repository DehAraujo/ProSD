# server/server.py
import zmq, json, os
from datetime import datetime

BROKER = "tcp://broker:5556"   # conecta no DEALER do broker
DATA_FILE = "/app/data/state.json"

os.makedirs("/app/data", exist_ok=True)
# inicializa storage
if not os.path.exists(DATA_FILE):
    with open(DATA_FILE, "w") as f:
        json.dump({"users": [], "channels": []}, f)

def load_state():
    with open(DATA_FILE, "r") as f:
        return json.load(f)

def save_state(state):
    with open(DATA_FILE, "w") as f:
        json.dump(state, f, indent=2)

ctx = zmq.Context()
rep = ctx.socket(zmq.REP)
rep.connect(BROKER)
print("Servidor (REP) conectado ao broker:", BROKER)

def now():
    return datetime.utcnow().isoformat()

while True:
    try:
        raw = rep.recv_json()
    except Exception as e:
        print("Erro recv:", e)
        continue

    svc = raw.get("service")
    data = raw.get("data", {})
    # Carrega estado atual
    state = load_state()

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
        rep.send_json({"service":"channels","data":{"timestamp":now(),"channels":[c["channel"] for c in state["channels"]]}})

    else:
        rep.send_json({"service":"error","data":{"timestamp":now(),"description":"unknown service"}})
