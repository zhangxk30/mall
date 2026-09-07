# orderprocessorservice

消费 `checkoutservice` 下单后发出的 `order-placed` Kafka 消息，把对应订单在
MySQL 里的 `status` 从 `PLACED` 更新为 `PROCESSED`。不对外提供 gRPC 接口，
没有 Service，只有一个 `/healthz` HTTP 探针。

跟 `checkoutservice` 共用同一个 `orders` 数据库/表（见
`../checkoutservice/mysql/schema.sql`），无需单独的 schema。

## 环境变量

| 变量 | 说明 | 默认值 |
| --- | --- | --- |
| `KAFKA_BROKERS` | Kafka broker 地址（逗号分隔），**必须设置**，否则进程直接退出 | - |
| `KAFKA_TOPIC` | 消费的 topic | `order-placed` |
| `MYSQL_HOST` | 数据库地址，未设置时只打日志、不更新订单状态 | - |
| `MYSQL_PORT` | 端口 | `3306` |
| `MYSQL_USER` | 用户名 | - |
| `MYSQL_PASSWORD` | 密码（生产环境建议通过 K8s Secret 注入） | - |
| `MYSQL_DATABASE` | 数据库名 | - |
| `PORT` | `/healthz` 监听端口 | `8081` |
| `EXTRA_LATENCY` | 处理每条消息前额外等待的时长（如 `500ms`），用于演示消息堆积 | - |
| `ENABLE_TRACING` | 设为 `1` 启用 APM 上报 | - |
| `APM_ENDPOINT` / `APM_TOKEN` / `APM_SERVICE_NAME` / `APM_HOST_NAME` | 与其它 Go 服务一致的 APM 配置 | - |

消费者从 Kafka 消息 header 里提取 `traceparent`（由 `checkoutservice` 的
`order_publisher.go` 注入），保证消费端的 span 挂在下单请求的 trace 下面。

对应的 Kubernetes 配置见 `../../kubernetes-manifests/orderprocessorservice.yaml`。
