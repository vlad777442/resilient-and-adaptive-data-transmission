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
# Server: 
# Start the server on default port (4242)
go run server.go

# Or specify an output directory and port
go run server.go -output /path/to/save/files -port 5000

# Client: 

# Send a single file
go run client.go -server localhost:4242 -input /path/to/file.txt

# Send all files from a directory
go run client.go -server localhost:4242 -input /path/to/directory
```


If needed

## Temporarily increase the maximum UDP buffer size
sudo sysctl -w net.core.rmem_max=7340032
sudo sysctl -w net.core.wmem_max=7340032

## Temporarily set the default UDP buffer size
sudo sysctl -w net.core.rmem_default=7340032
sudo sysctl -w net.core.wmem_default=7340032

## Go Installation

wget https://go.dev/dl/go1.24.2.linux-amd64.tar.gz

sudo tar -C /usr/local -xzf go1.24.2.linux-amd64.tar.gz

nano ~/.bashrc

Add in the end

export PATH=/usr/local/go/bin:$PATH

reload:
source ~/.bashrc
