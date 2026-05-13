# Dev Environment

## Start

```sh
cargo build --features mqtt
cd dev && docker compose up -d
```

## Watch (auto-restart on rebuild)

```sh
cargo watch -x 'build --features mqtt' -s 'docker compose -f dev/docker-compose.yml restart bridge'
```

## Regenerate proto files

```sh
protoc --proto_path=dev/mqtt --include_imports --descriptor_set_out=dev/mqtt/factory.desc factory.proto
protoc --proto_path=dev/mqtt --python_out=dev/mqtt factory.proto
```
