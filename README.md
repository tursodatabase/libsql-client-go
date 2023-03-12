# Running examples

Using HTTPS:
```console
go run -ldflags="-X 'main.dbUrl=https://<login>:<password>@<sqld db url>'" examples/sql/counter/main.go
```

Using Websockets:
```console
go run -ldflags="-X 'main.dbUrl=wss://<sqld db url>:2023?jwt=<jwt authorization token>'" examples/sql/counter/main.go
```
