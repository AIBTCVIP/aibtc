<img width="1918" height="910" alt="395d276fb726828d1f72c8e03e85b1d4" src="https://github.com/user-attachments/assets/5ffaec8c-03c0-4a26-8224-d9cd587aaf61" />

<img width="1918" height="910" alt="80e810d35bc58edd2adbc86cd5cf1c34" src="https://github.com/user-attachments/assets/515a6961-0c73-407b-ab6f-e8cddbca9acd" />

<img width="920" height="778" alt="image" src="https://github.com/user-attachments/assets/7226572f-c95a-4dbe-b3eb-d96dfc2e165a" />


# AIBTC

多用户 AI 量化交易系统，支持多交易所、多策略的自动化交易。

## 功能特性

- **多交易所支持**: Binance、OKX、Bitget、Hyperliquid
- **AI 智能交易**: 集成 LLM 进行市场分析和交易决策
- **多用户架构**: 支持多用户独立运行，用户间完全隔离
- **策略模板**: 预设多种交易策略，支持自定义覆盖
- **推荐返佣**: 推荐返佣系统 (未完善)
- **实时通知**: Telegram 交易通知
- **Web 管理**: 完整的 Web 管理界面

## 技术栈

- **后端**: Python 3.10+, FastAPI, asyncio
- **数据库**: MySQL 8.0+
- **缓存**: Redis
- **前端**: Vue 3, Tailwind CSS

## 快速开始

### 1. 环境要求

- Python 3.10+
- MySQL 8.0+
- Redis 6.0+

### 2. 安装

# 运行安装脚本
python scripts/setup.py
```

### 3. 创建数据库

```bash
mysql -u root -p -e "CREATE DATABASE tradev6 CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci"
```

### 4. 启动服务

```bash
python main_async.py
```

服务启动后访问: http://localhost:8080

## 项目结构

```
aibtc/
├── api/                # Web API
│   ├── web.py          # FastAPI 应用
│   ├── user_api.py     # 用户 API
│   ├── admin_api.py    # 管理员 API
│   └── referral_api.py # 推荐返佣 API
├── core/               # 核心模块
│   ├── user_db.py      # 用户数据库
│   ├── lifecycle.py    # 生命周期管理
│   └── config.py       # 配置管理
├── exchanges/          # 交易所接口
│   ├── binance/
│   ├── okx/
│   ├── bitget/
│   └── hyperliquid/
├── llm/                # LLM 集成
│   └── context_builder.py
├── trading/            # 交易逻辑
├── analysis/           # 市场分析
├── notifications/      # 通知服务
├── static/             # 前端静态文件
├── scripts/            # 工具脚本
│   └── setup.py        # 安装脚本
├── sql/                # 数据库脚本
│   └── schema.sql      # 表结构
├── main_async.py       # 启动入口
├── requirements.txt    # Python 依赖
└── .env                # 环境配置
```

## 配置说明

主要配置项在 `.env` 文件中:

| 配置项 | 说明 |
|--------|------|
| DATABASE_URL | MySQL 连接字符串 |
| JWT_SECRET | JWT 签名密钥 |
| ENCRYPTION_KEY | 数据加密密钥 |
| REDIS_URL | Redis 连接地址 |
| TELEGRAM_BOT_TOKEN | Telegram 机器人 Token |

```bash
# 安装依赖
pip install -r requirements.txt

# 开发模式启动
python main_async.py

管理员账号名称:

aibtcvip (自行注册一个账号)

```
