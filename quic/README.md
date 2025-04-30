## Go to project folder
```
cd server

or

cd client
```
## Initialize Go module
```
go mod init quic-file-transfer
```
## Get dependencies
```
go get github.com/quic-go/quic-go
```
## To build the server
```
go build -o quic-server server.go
```

## To build the client
```
go build -o quic-client client.go
```

## To run
```
Server: 
./quic-server -port 4242 -output /path/to/save/files

Client: 
./quic-client -server server_ip:4242 -file /path/to/file/to/send
```


If needed

## Temporarily increase the maximum UDP buffer size
sudo sysctl -w net.core.rmem_max=7340032
sudo sysctl -w net.core.wmem_max=7340032

## Temporarily set the default UDP buffer size
sudo sysctl -w net.core.rmem_default=7340032
sudo sysctl -w net.core.wmem_default=7340032