# Running examples

Using HTTPS:
```console
go run -ldflags="-X 'main.dbUrl=https://<login>:<password>@<sqld db url>'" examples/sql-driver/counter/main.go
```

Using Websockets:
```console
export SQLD_AUTH_TOKEN=<jwt authorization token>
go run -ldflags="-X 'main.dbUrl=wss://<sqld db url>:2023'" examples/sql-driver/counter/main.go
```
