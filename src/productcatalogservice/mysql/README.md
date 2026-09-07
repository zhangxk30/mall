# MySQL 商品数据导入

将 `../products.json` 中的商品数据导入腾讯云 MySQL，供 `productcatalogservice` 通过
`MYSQL_HOST` 等环境变量读取（见 `../catalog_loader.go` 的 `loadCatalogFromMySQL`）。

## 前提

- 已创建好腾讯云 MySQL 实例，并有一个可写入的数据库（下例假设数据库名为 `productcatalog`）。
- 本机能访问该实例（内网/公网 + 白名单已放通）。

## 导入步骤

```bash
mysql -h <MYSQL_HOST> -P <MYSQL_PORT> -u <MYSQL_USER> -p <MYSQL_DATABASE> < schema.sql
mysql -h <MYSQL_HOST> -P <MYSQL_PORT> -u <MYSQL_USER> -p <MYSQL_DATABASE> < seed.sql
```

## 校验

```bash
mysql -h <MYSQL_HOST> -P <MYSQL_PORT> -u <MYSQL_USER> -p <MYSQL_DATABASE> -e "SELECT id, name FROM products;"
```

应看到 9 条商品记录，与 `../products.json` 一致。

## 服务侧配置

`productcatalogservice` 需要以下环境变量才会从 MySQL 读取数据（未设置时回退到本地 `products.json`）：

| 变量 | 说明 | 默认值 |
| --- | --- | --- |
| `MYSQL_HOST` | 数据库地址，设置后即启用 MySQL 数据源 | - |
| `MYSQL_PORT` | 端口 | `3306` |
| `MYSQL_USER` | 用户名 | - |
| `MYSQL_PASSWORD` | 密码（生产环境建议通过 K8s Secret 注入） | - |
| `MYSQL_DATABASE` | 数据库名 | - |
| `MYSQL_TABLE` | 表名 | `products` |

对应的 Kubernetes 配置见 `../../../kubernetes-manifests/productcatalogservice.yaml` 和
`../../../kubernetes-manifests/mysql-credentials-secret.yaml`。
