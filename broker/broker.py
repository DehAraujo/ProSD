import zmq

ctx = zmq.Context()

# Frontend (clientes e bots)
frontend = ctx.socket(zmq.ROUTER)
frontend.bind("tcp://*:5555")

# Backend (servidores)
backend = ctx.socket(zmq.DEALER)
backend.bind("tcp://*:5556")

print("🧩 Broker ativo! Encaminhando mensagens entre clientes e servidores...")

zmq.proxy(frontend, backend)
