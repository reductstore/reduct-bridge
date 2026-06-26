# Dev Environment

Each subdirectory contains a self-contained `docker-compose.yml` and config for a specific input type.

## MQTT

```sh
cargo build --features mqtt
docker compose -f dev/mqtt/docker-compose.yml up
```

### Regenerate proto files

```sh
protoc --proto_path=dev/mqtt --include_imports --descriptor_set_out=dev/mqtt/factory.desc factory.proto
protoc --proto_path=dev/mqtt --python_out=dev/mqtt factory.proto
```

## HTTP

```sh
cargo build --features http
docker compose -f dev/http/docker-compose.yml up
```
