# MySQL 物流单数据

`shippingservice` 每次 `ShipOrder` 成功生成 tracking ID 后，把物流单写入
这张表（见 `../shipment_store.go`）。**只写不读**，网站上没有查询入口。

## 前提

- 已创建好腾讯云 MySQL 实例，并有一个可写入的数据库（下例假设数据库名为 `shipping`）。
- 本机能访问该实例（内网/公网 + 白名单已放通）。

## 导入步骤

```bash
mysql -h <MYSQL_HOST> -P <MYSQL_PORT> -u <MYSQL_USER> -p <MYSQL_DATABASE> < schema.sql
```

## 校验

```bash
mysql -h <MYSQL_HOST> -P <MYSQL_PORT> -u <MYSQL_USER> -p <MYSQL_DATABASE> \
  -e "SELECT tracking_id, city, item_count, created_at FROM shipments ORDER BY created_at DESC LIMIT 10;"
```

## 服务侧配置

`shippingservice` 需要以下环境变量才会把物流单写入 MySQL（未设置时只打日志、
不写库，`ShipOrder` 主流程不受影响）：

| 变量 | 说明 | 默认值 |
| --- | --- | --- |
| `MYSQL_HOST` | 数据库地址，设置后即启用物流单落库 | - |
| `MYSQL_PORT` | 端口 | `3306` |
| `MYSQL_USER` | 用户名 | - |
| `MYSQL_PASSWORD` | 密码（生产环境建议通过 K8s Secret 注入） | - |
| `MYSQL_DATABASE` | 数据库名 | - |

对应的 Kubernetes 配置见 `../../../kubernetes-manifests/shippingservice.yaml` 和
`../../../kubernetes-manifests/mysql-credentials-secret.yaml`。
