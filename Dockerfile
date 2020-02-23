FROM golang:latest as builder
LABEL maintainer="Woojay Poynter <h770nul@gmail.com>"
WORKDIR /app

COPY go.mod ./
RUN go mod download
COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -o nbbo-receiver .

FROM alpine:latest  
RUN apk --no-cache add ca-certificates

WORKDIR /root/
COPY --from=builder /app/nbbo-receiver .

EXPOSE 2000

CMD ["./nbbo-receiver"]