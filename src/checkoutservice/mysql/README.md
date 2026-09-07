# MySQL 订单数据

`checkoutservice` 下单成功后把订单写入这两张表（见 `../order_store.go`），
`orderprocessorservice` 消费 Kafka 消息后把 `orders.status` 从 `PLACED`
更新为 `PROCESSED`（见 `../../orderprocessorservice/order_store.go`）。

**这是只写不读的数据**：网站上没有订单查询页面，数据只能通过直接查库或
腾讯云控制台查看。

## 前提

- 已创建好腾讯云 MySQL 实例，并有一个可写入的数据库（下例假设数据库名为 `orders`）。
- 本机能访问该实例（内网/公网 + 白名单已放通）。
- `checkoutservice` 和 `orderprocessorservice` 需要连接**同一个**数据库/表。

## 导入步骤

```bash
mysql -h <MYSQL_HOST> -P <MYSQL_PORT> -u <MYSQL_USER> -p <MYSQL_DATABASE> < schema.sql
```

## 校验

```bash
mysql -h <MYSQL_HOST> -P <MYSQL_PORT> -u <MYSQL_USER> -p <MYSQL_DATABASE> \
  -e "SELECT order_id, user_id, status, created_at FROM orders ORDER BY created_at DESC LIMIT 10;"
```

下单后应能看到状态为 `PLACED` 的新订单；`orderprocessorservice` 消费到对应
Kafka 消息后，状态会变为 `PROCESSED`。

## 服务侧配置

`checkoutservice` 需要以下环境变量才会把订单写入 MySQL 并发送 Kafka 消息
（`MYSQL_HOST` 未设置时只打日志、不写库；`KAFKA_BROKERS` 未设置时只打日志、
不发消息；下单主流程不受影响）：

| 变量 | 说明 | 默认值 |
| --- | --- | --- |
| `MYSQL_HOST` | 数据库地址，设置后即启用订单落库 | - |
| `MYSQL_PORT` | 端口 | `3306` |
| `MYSQL_USER` | 用户名 | - |
| `MYSQL_PASSWORD` | 密码（生产环境建议通过 K8s Secret 注入） | - |
| `MYSQL_DATABASE` | 数据库名 | - |
| `KAFKA_BROKERS` | Kafka broker 地址（逗号分隔），设置后即发送下单事件 | - |
| `KAFKA_TOPIC` | Topic 名 | `order-placed` |

`orderprocessorservice` 需要相同的 `MYSQL_*` 变量（连同一个库）以及
`KAFKA_BROKERS` / `KAFKA_TOPIC` 来消费消息，见
`../../orderprocessorservice/README.md`。

对应的 Kubernetes 配置见 `../../../kubernetes-manifests/checkoutservice.yaml`、
`../../../kubernetes-manifests/mysql-credentials-secret.yaml` 和
`../../../kubernetes-manifests/kafka-credentials-secret.yaml`。
