# Go SDK for libSQL

[![License](https://img.shields.io/badge/license-MIT-blue)](https://github.com/libsql/libsql-client-go/blob/main/LICENSE)

This is the source repository of the Go SDK for libSQL. You can either connect to a local SQLite/libSQL database (embedded in the client) or to a remote libSQL server.

## Running examples

Using HTTPS:
```console
go run -ldflags="-X 'main.dbUrl=https://<login>:<password>@<sqld db url>'" examples/sql/counter/main.go
```

Using Websockets:
```console
go run -ldflags="-X 'main.dbUrl=wss://<sqld db url>:2023?jwt=<jwt authorization token>'" examples/sql/counter/main.go
```

## License

This project is licensed under the MIT license.

### Contribution

Unless you explicitly state otherwise, any contribution intentionally submitted for inclusion in `sqld` by you, shall be licensed as MIT, without any additional terms or conditions.
