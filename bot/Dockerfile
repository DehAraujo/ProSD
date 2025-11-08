FROM golang:1.22

WORKDIR /app

# Instala dependências do sistema, incluindo libzmq
RUN apt-get update && apt-get install -y libzmq3-dev pkg-config

# Copia o código-fonte
COPY bot.go .

# Inicializa módulo e compila
RUN go mod init bot && go get github.com/pebbe/zmq4 && go build -o bot .

CMD ["./bot"]
