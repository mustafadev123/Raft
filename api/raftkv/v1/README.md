# API generation

Generate the checked-in Go and gRPC stubs from the repository root:

```powershell
protoc --go_out=. --go_opt=paths=source_relative `
  --go-grpc_out=. --go-grpc_opt=paths=source_relative `
  api/raftkv/v1/raftkv.proto
```

The generated files currently use `protoc` 36.2, `protoc-gen-go` 1.36.12, and
`protoc-gen-go-grpc` 1.6.2.

