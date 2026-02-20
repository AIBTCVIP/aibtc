#!/usr/bin/env python3
"""
TradeV6 项目安装配置脚本
用法: python scripts/setup.py
"""
import os
import sys
import secrets
import subprocess

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
ENV_FILE = os.path.join(PROJECT_ROOT, ".env")
ENV_EXAMPLE = os.path.join(PROJECT_ROOT, ".env.example")
REQUIREMENTS = os.path.join(PROJECT_ROOT, "requirements.txt")


def generate_secret(length=64):
    """生成安全随机密钥"""
    return secrets.token_urlsafe(length)


def generate_hex_secret(length=64):
    """生成十六进制密钥"""
    return secrets.token_hex(length)


def install_dependencies():
    """安装 Python 依赖"""
    print("\n[1/3] 安装 Python 依赖...")
    try:
        subprocess.check_call([sys.executable, "-m", "pip", "install", "-r", REQUIREMENTS, "-q"])
        print("    依赖安装完成")
        return True
    except subprocess.CalledProcessError as e:
        print(f"    依赖安装失败: {e}")
        return False


def configure_env():
    """交互式配置 .env 文件"""
    print("\n[2/3] 配置环境变量...")
    
    config = {}
    
    # MySQL 数据库配置
    print("\n--- MySQL 数据库配置 ---")
    host = input("MySQL 主机 (默认 127.0.0.1): ").strip() or "127.0.0.1"
    port = input("MySQL 端口 (默认 3306): ").strip() or "3306"
    user = input("MySQL 用户名 (默认 root): ").strip() or "root"
    password = input("MySQL 密码: ").strip() or "123456"
    database = input("数据库名 (默认 tradev5): ").strip() or "tradev5"
    
    config["DATABASE_URL"] = f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}?charset=utf8mb4"
    config["MYSQL_HOST"] = host
    config["MYSQL_PORT"] = port
    config["MYSQL_USER"] = user
    config["MYSQL_PASSWORD"] = password
    config["MYSQL_DATABASE"] = database
    
    # 安全密钥 (自动生成)
    print("\n--- 安全密钥 (自动生成) ---")
    config["JWT_SECRET"] = generate_secret(64)
    config["PASSWORD_SALT"] = generate_hex_secret(64)
    config["ENCRYPTION_KEY"] = generate_secret(32)
    print("    JWT_SECRET: 已生成")
    print("    PASSWORD_SALT: 已生成")
    print("    ENCRYPTION_KEY: 已生成")
    
    # CORS 配置
    print("\n--- CORS 配置 ---")
    cors = input("允许的来源 (默认 http://localhost:8080): ").strip()
    config["CORS_ORIGINS"] = cors or "http://localhost:8080,http://127.0.0.1:8080"
    
    # Redis 配置
    print("\n--- Redis 配置 ---")
    redis_url = input("Redis URL (默认 redis://127.0.0.1:6379/0): ").strip()
    config["REDIS_URL"] = redis_url or "redis://127.0.0.1:6379/0"
    
    # Telegram 配置 (可选)
    print("\n--- Telegram 配置 (可选，直接回车跳过) ---")
    tg_token = input("Telegram Bot Token: ").strip()
    tg_chat = input("Telegram Chat ID: ").strip()
    config["TELEGRAM_BOT_TOKEN"] = tg_token or ""
    config["TELEGRAM_CHAT_ID"] = tg_chat or ""
    
    # 日志配置
    config["LOG_LEVEL"] = "INFO"
    config["LOG_DIR"] = "logs"
    
    # 性能优化
    config["USE_GLOBAL_SLP"] = "1"
    config["USE_GLOBAL_MARK_UPDATER"] = "1"
    config["USE_GLOBAL_AUDITOR"] = "1"
    
    return config


def write_env(config):
    """写入 .env 文件"""
    print("\n[3/3] 写入配置文件...")
    
    content = f"""# TradeV6 环境配置
# 由 setup.py 自动生成

# ==========================================================
# 数据库配置
# ==========================================================
DATABASE_URL={config.get('DATABASE_URL', '')}
"""
    
    if config.get('MYSQL_HOST'):
        content += f"""
MYSQL_HOST={config['MYSQL_HOST']}
MYSQL_PORT={config['MYSQL_PORT']}
MYSQL_USER={config['MYSQL_USER']}
MYSQL_PASSWORD={config['MYSQL_PASSWORD']}
MYSQL_DATABASE={config['MYSQL_DATABASE']}
"""
    
    content += f"""
# ==========================================================
# 安全配置
# ==========================================================
JWT_SECRET={config['JWT_SECRET']}
PASSWORD_SALT={config['PASSWORD_SALT']}
ENCRYPTION_KEY={config['ENCRYPTION_KEY']}
CORS_ORIGINS={config['CORS_ORIGINS']}

# ==========================================================
# Redis 配置
# ==========================================================
REDIS_URL={config['REDIS_URL']}

# ==========================================================
# Telegram 配置
# ==========================================================
TELEGRAM_BOT_TOKEN={config.get('TELEGRAM_BOT_TOKEN', '')}
TELEGRAM_CHAT_ID={config.get('TELEGRAM_CHAT_ID', '')}

# ==========================================================
# 日志配置
# ==========================================================
LOG_LEVEL={config['LOG_LEVEL']}
LOG_DIR={config['LOG_DIR']}

# ==========================================================
# 性能优化
# ==========================================================
USE_GLOBAL_SLP={config['USE_GLOBAL_SLP']}
USE_GLOBAL_MARK_UPDATER={config['USE_GLOBAL_MARK_UPDATER']}
USE_GLOBAL_AUDITOR={config['USE_GLOBAL_AUDITOR']}
"""
    
    # 备份旧文件
    if os.path.exists(ENV_FILE):
        backup = ENV_FILE + ".backup"
        os.rename(ENV_FILE, backup)
        print(f"    旧配置已备份到 {backup}")
    
    with open(ENV_FILE, "w", encoding="utf-8") as f:
        f.write(content)
    
    print(f"    配置已写入 {ENV_FILE}")


def main():
    print("=" * 50)
    print("  TradeV6 项目安装配置")
    print("=" * 50)
    
    # 1. 安装依赖
    if not install_dependencies():
        print("\n依赖安装失败，请手动运行: pip install -r requirements.txt")
    
    # 2. 配置环境
    config = configure_env()
    
    # 3. 写入配置
    write_env(config)
    
    print("\n" + "=" * 50)
    print("  配置完成!")
    print("=" * 50)
    print("\n下一步:")
    print("  1. 创建数据库: mysql -u root -p -e 'CREATE DATABASE tradev5'")
    print("  2. 初始化表: python -c \"from core.user_db import get_user_db; get_user_db()\"")
    print("  3. 启动服务: python main_async.py")


if __name__ == "__main__":
    main()
