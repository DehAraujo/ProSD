import zmq

ctx = zmq.Context()

xsub = ctx.socket(zmq.XSUB)
xsub.bind("tcp://*:5557")  # Recebe publicações dos servidores

xpub = ctx.socket(zmq.XPUB)
xpub.bind("tcp://*:5558")  # Envia para os subscribers

print("📡 Proxy PUB/SUB ativo (XSUB↔XPUB)!")
zmq.proxy(xsub, xpub)
