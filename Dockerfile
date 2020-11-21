FROM golang as stage0

ENV GO111MODULE=on
WORKDIR /
COPY go.mod .
COPY go.sum .
RUN go mod download

WORKDIR /app/bench/
COPY . .

RUN make bench

COPY entrypoint.sh /

#ENTRYPOINT ["/entrypoint.sh"]
CMD tail -f /dev/null
