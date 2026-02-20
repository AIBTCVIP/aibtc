-- ============================================================
-- TradeV6 多用户交易系统数据库 Schema
-- 版本: v5.0
-- 支持: MySQL 8.0+ / MariaDB 10.5+
-- 
-- 使用方式:
--   mysql -u root -p tradev6 < sql/schema.sql
-- 
-- 或者使用 Python 脚本:
--   python scripts/create_mysql_tables.py
-- ============================================================

SET NAMES utf8mb4;
SET FOREIGN_KEY_CHECKS = 0;

-- ============================================================
-- 1. 用户表
-- ============================================================
CREATE TABLE IF NOT EXISTS users (
    id INT AUTO_INCREMENT PRIMARY KEY,
    uid VARCHAR(64) UNIQUE NOT NULL COMMENT '用户唯一标识',
    username VARCHAR(64) UNIQUE NOT NULL COMMENT '登录用户名',
    password_hash VARCHAR(256) NOT NULL COMMENT '密码哈希',
    email VARCHAR(128) COMMENT '邮箱',
    totp_secret_encrypted TEXT COMMENT 'Google Authenticator 密钥 (AES加密)',
    status VARCHAR(20) DEFAULT 'active' COMMENT '账户状态: active/suspended/deleted',
    tier VARCHAR(20) DEFAULT 'free' COMMENT '用户等级: free/basic/pro/vip',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    
    INDEX idx_username (username),
    INDEX idx_status (status),
    INDEX idx_tier (tier)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
COMMENT='用户表';


-- ============================================================
-- 2. 用户交易所配置表 (多交易所支持)
-- ============================================================
CREATE TABLE IF NOT EXISTS user_exchange_config (
    id INT AUTO_INCREMENT PRIMARY KEY,
    uid VARCHAR(64) NOT NULL COMMENT '用户ID',
    exchange VARCHAR(32) NOT NULL COMMENT '交易所: binance/okx/bitget/hyperliquid',
    
    -- API 密钥 (AES加密存储)
    api_key_encrypted TEXT COMMENT 'API Key (AES加密)',
    api_secret_encrypted TEXT COMMENT 'API Secret (AES加密)',
    passphrase_encrypted TEXT COMMENT 'Passphrase (OKX/Bitget需要, AES加密)',
    wallet_address VARCHAR(256) COMMENT '钱包地址 (Hyperliquid可选)',
    
    -- 交易所配置
    is_testnet TINYINT DEFAULT 0 COMMENT '是否测试网',
    is_enabled TINYINT DEFAULT 0 COMMENT '是否启用',
    is_running TINYINT DEFAULT 0 COMMENT '是否正在运行',
    leverage INT DEFAULT 20 COMMENT '杠杆倍数',
    monitor_symbols TEXT COMMENT '监控币种列表 (JSON)',
    ai500_enabled TINYINT DEFAULT 0 COMMENT 'AI500智能选币',
    ai_strategy_id VARCHAR(64) COMMENT '使用的AI策略ID',
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    
    UNIQUE KEY uk_uid_exchange (uid, exchange),
    INDEX idx_uid (uid),
    INDEX idx_is_running (is_running),
    INDEX idx_ai_strategy_id (ai_strategy_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
COMMENT='用户交易所配置表 - 每个用户可配置多个交易所';


-- ============================================================
-- 3. 用户交易配置表 (全局配置)
-- ============================================================
CREATE TABLE IF NOT EXISTS user_trading_config (
    id INT AUTO_INCREMENT PRIMARY KEY,
    uid VARCHAR(64) UNIQUE NOT NULL COMMENT '用户ID',
    
    -- 交易参数
    max_positions INT DEFAULT 30 COMMENT '最大同时持仓数',
    position_size_pct DECIMAL(10,4) DEFAULT 3.0 COMMENT '单笔仓位比例 (%)',
    default_leverage INT DEFAULT 30 COMMENT '默认杠杆倍数',
    monitor_symbols TEXT COMMENT '监控币种列表 (JSON)',
    
    -- AI 配置
    ai_enabled TINYINT DEFAULT 0 COMMENT '是否启用AI交易',
    ai500_enabled TINYINT DEFAULT 0 COMMENT 'AI500智能选币',
    trading_enabled TINYINT DEFAULT 0 COMMENT '是否启用交易',
    
    -- LLM 配置 (全局默认)
    llm_provider VARCHAR(64) DEFAULT 'anthropic' COMMENT 'LLM提供商',
    llm_model VARCHAR(128) DEFAULT '' COMMENT '模型名称',
    llm_api_key_encrypted TEXT COMMENT 'LLM API Key (AES加密)',
    llm_base_url VARCHAR(512) COMMENT '自定义API地址',
    system_prompt TEXT COMMENT '系统提示词 (已废弃, 使用 user_ai_strategies)',
    temperature DECIMAL(10,4) DEFAULT 0.01 COMMENT 'LLM温度参数',
    max_tokens INT DEFAULT 8000 COMMENT '最大输出tokens',
    
    -- Telegram 通知配置
    telegram_bot_token_encrypted TEXT COMMENT 'Telegram Bot Token (AES加密)',
    telegram_chat_id VARCHAR(64) COMMENT 'Telegram Chat ID',
    telegram_topic_id VARCHAR(64) COMMENT 'Telegram 群组话题 ID',
    telegram_enabled TINYINT DEFAULT 0 COMMENT '是否启用Telegram通知',
    
    -- 执行约束
    min_rr_ratio DECIMAL(10,4) DEFAULT 3.0 COMMENT '最小风险回报比',
    limit_order_min_distance_pct DECIMAL(10,4) DEFAULT 3.0 COMMENT '限价单最小距离 (%)',
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    
    INDEX idx_uid (uid),
    INDEX idx_ai_enabled (ai_enabled),
    INDEX idx_trading_enabled (trading_enabled)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
COMMENT='用户交易配置表 - 全局交易参数';


-- ============================================================
-- 4. 用户 AI 策略表 (v5.0)
-- ============================================================
CREATE TABLE IF NOT EXISTS user_ai_strategies (
    id INT AUTO_INCREMENT PRIMARY KEY,
    uid VARCHAR(64) NOT NULL COMMENT '用户ID',
    strategy_id VARCHAR(64) NOT NULL COMMENT '策略ID',
    name VARCHAR(128) NOT NULL COMMENT '策略名称',
    
    -- LLM 配置 (每个策略可以使用不同的模型)
    llm_provider VARCHAR(64) DEFAULT 'anthropic' COMMENT 'LLM提供商',
    llm_model VARCHAR(128) DEFAULT '' COMMENT '模型名称',
    llm_api_key_encrypted TEXT COMMENT 'LLM API Key (AES加密)',
    llm_base_url VARCHAR(512) COMMENT '自定义API地址',
    
    -- LLM 参数 (可选, NULL表示使用提供商默认值)
    temperature DECIMAL(3,2) DEFAULT NULL COMMENT 'Temperature参数 (0-2)',
    top_p DECIMAL(3,2) DEFAULT NULL COMMENT 'Top P参数 (0-1)',
    max_tokens INT DEFAULT NULL COMMENT '最大输出tokens',
    
    -- 策略配置 (v5.0)
    strategy_preset VARCHAR(64) COMMENT '预设策略: conservative/aggressive/trend_following/mean_reversion/default',
    strategy_overrides LONGTEXT COMMENT '自定义覆盖 (JSON: {category: content})',
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    
    UNIQUE KEY uk_uid_strategy (uid, strategy_id),
    INDEX idx_uid (uid),
    INDEX idx_strategy_preset (strategy_preset)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
COMMENT='用户AI策略表 - 每个策略包含LLM配置和交易策略 (v5.0)';


-- ============================================================
-- 5. 策略模板表 (系统预设)
-- ============================================================
CREATE TABLE IF NOT EXISTS strategy_templates (
    id INT AUTO_INCREMENT PRIMARY KEY,
    preset_name VARCHAR(64) NOT NULL COMMENT '预设名称',
    category VARCHAR(32) NOT NULL COMMENT '分类: role/risk_rules/entry_conditions/exit_conditions/position_sizing/market_preferences/adaptive_rules',
    content TEXT NOT NULL COMMENT '模板内容',
    display_order INT DEFAULT 0 COMMENT '显示顺序',
    is_enabled TINYINT DEFAULT 1 COMMENT '是否启用',
    description VARCHAR(256) COMMENT '预设描述',
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    
    UNIQUE KEY uk_preset_category (preset_name, category),
    INDEX idx_preset_name (preset_name),
    INDEX idx_category (category),
    INDEX idx_is_enabled (is_enabled)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
COMMENT='LLM策略模板表 - 系统预设策略，按分类存储';


-- ============================================================
-- 6. AI 决策历史表
-- ============================================================
CREATE TABLE IF NOT EXISTS ai_decisions (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    uid VARCHAR(64) NOT NULL COMMENT '用户ID',
    timestamp DOUBLE NOT NULL COMMENT '时间戳 (毫秒)',
    
    -- 请求/响应数据 (使用 LONGTEXT 保持 JSON key 顺序)
    request_data LONGTEXT COMMENT '请求数据 (JSON)',
    response_data LONGTEXT COMMENT '响应数据 (JSON)',
    
    -- 统计信息
    signals_count INT DEFAULT 0 COMMENT '信号数量',
    total_tokens INT DEFAULT 0 COMMENT '总 tokens',
    prompt_tokens INT DEFAULT 0 COMMENT 'Prompt tokens',
    completion_tokens INT DEFAULT 0 COMMENT 'Completion tokens',
    response_time_ms INT DEFAULT 0 COMMENT '响应时间 (毫秒)',
    http_status INT DEFAULT 200 COMMENT 'HTTP 状态码',
    llm_model VARCHAR(128) COMMENT '使用的模型',
    
    -- 分组信息
    is_grouped TINYINT DEFAULT 0 COMMENT '是否分组请求',
    group_count INT DEFAULT 1 COMMENT '分组数量',
    
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    
    INDEX idx_uid_timestamp (uid, timestamp DESC),
    INDEX idx_uid_created (uid, created_at DESC),
    INDEX idx_timestamp (timestamp DESC)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
COMMENT='AI决策历史表';


-- ============================================================
-- 7. 用户推荐关系表
-- ============================================================
CREATE TABLE IF NOT EXISTS user_referrals (
    id INT AUTO_INCREMENT PRIMARY KEY,
    uid VARCHAR(64) UNIQUE NOT NULL COMMENT '用户ID',
    referrer_uid VARCHAR(64) COMMENT '推荐人ID',
    referral_code VARCHAR(32) UNIQUE NOT NULL COMMENT '邀请码',
    referred_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '注册时间',
    
    -- 15级推荐链
    level_1_uid VARCHAR(64),
    level_2_uid VARCHAR(64),
    level_3_uid VARCHAR(64),
    level_4_uid VARCHAR(64),
    level_5_uid VARCHAR(64),
    level_6_uid VARCHAR(64),
    level_7_uid VARCHAR(64),
    level_8_uid VARCHAR(64),
    level_9_uid VARCHAR(64),
    level_10_uid VARCHAR(64),
    level_11_uid VARCHAR(64),
    level_12_uid VARCHAR(64),
    level_13_uid VARCHAR(64),
    level_14_uid VARCHAR(64),
    level_15_uid VARCHAR(64),
    
    INDEX idx_referrer_uid (referrer_uid),
    INDEX idx_referral_code (referral_code)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
COMMENT='用户推荐关系表';


-- ============================================================
-- 8. 推荐返佣比例表
-- ============================================================
CREATE TABLE IF NOT EXISTS referral_commission_rates (
    id INT AUTO_INCREMENT PRIMARY KEY,
    level INT UNIQUE NOT NULL COMMENT '层级 (1-15)',
    rate DECIMAL(10,6) DEFAULT 0 COMMENT '返佣比例',
    min_amount DECIMAL(20,8) DEFAULT 0 COMMENT '最小返佣金额',
    is_enabled TINYINT DEFAULT 1 COMMENT '是否启用',
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    updated_by VARCHAR(64) COMMENT '更新者'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
COMMENT='推荐返佣比例表';


-- ============================================================
-- 9. 推荐返佣记录表
-- ============================================================
CREATE TABLE IF NOT EXISTS referral_commissions (
    id INT AUTO_INCREMENT PRIMARY KEY,
    uid VARCHAR(64) NOT NULL COMMENT '获得返佣的用户ID',
    from_uid VARCHAR(64) NOT NULL COMMENT '产生返佣的用户ID',
    level INT NOT NULL COMMENT '层级',
    source_type VARCHAR(32) NOT NULL COMMENT '来源类型: trade/subscription',
    source_id VARCHAR(128) COMMENT '来源ID',
    source_amount DECIMAL(20,8) NOT NULL COMMENT '来源金额',
    commission_rate DECIMAL(10,6) NOT NULL COMMENT '返佣比例',
    commission_amount DECIMAL(20,8) NOT NULL COMMENT '返佣金额',
    status VARCHAR(20) DEFAULT 'pending' COMMENT '状态: pending/settled/cancelled',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    settled_at TIMESTAMP NULL COMMENT '结算时间',
    notes TEXT COMMENT '备注',
    
    INDEX idx_uid (uid),
    INDEX idx_from_uid (from_uid),
    INDEX idx_status (status),
    INDEX idx_created_at (created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
COMMENT='推荐返佣记录表';


-- ============================================================
-- 10. 用户返佣余额表
-- ============================================================
CREATE TABLE IF NOT EXISTS user_commission_balance (
    id INT AUTO_INCREMENT PRIMARY KEY,
    uid VARCHAR(64) UNIQUE NOT NULL COMMENT '用户ID',
    total_earned DECIMAL(20,8) DEFAULT 0 COMMENT '总收入',
    total_withdrawn DECIMAL(20,8) DEFAULT 0 COMMENT '总提现',
    available_balance DECIMAL(20,8) DEFAULT 0 COMMENT '可用余额',
    frozen_balance DECIMAL(20,8) DEFAULT 0 COMMENT '冻结余额',
    last_settlement_at TIMESTAMP NULL COMMENT '最后结算时间',
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    
    INDEX idx_uid (uid)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
COMMENT='用户返佣余额表';


-- ============================================================
-- 11. 返佣提现记录表
-- ============================================================
CREATE TABLE IF NOT EXISTS commission_withdrawals (
    id INT AUTO_INCREMENT PRIMARY KEY,
    uid VARCHAR(64) NOT NULL COMMENT '用户ID',
    amount DECIMAL(20,8) NOT NULL COMMENT '提现金额',
    status VARCHAR(20) DEFAULT 'pending' COMMENT '状态: pending/processing/completed/rejected',
    withdraw_address VARCHAR(256) NOT NULL COMMENT '提现地址',
    withdraw_type VARCHAR(32) DEFAULT 'USDT' COMMENT '提现类型',
    tx_hash VARCHAR(256) COMMENT '交易哈希',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    processed_at TIMESTAMP NULL COMMENT '处理时间',
    processed_by VARCHAR(64) COMMENT '处理人',
    notes TEXT COMMENT '备注',
    
    INDEX idx_uid (uid),
    INDEX idx_status (status)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
COMMENT='返佣提现记录表';


-- ============================================================
-- 12. 用户盈利统计表
-- ============================================================
CREATE TABLE IF NOT EXISTS user_profit_stats (
    id INT AUTO_INCREMENT PRIMARY KEY,
    uid VARCHAR(64) NOT NULL COMMENT '用户ID',
    period_type VARCHAR(20) NOT NULL COMMENT '周期类型: daily/weekly/monthly/yearly',
    period_start TIMESTAMP NOT NULL COMMENT '周期开始',
    period_end TIMESTAMP NOT NULL COMMENT '周期结束',
    
    -- 盈亏统计
    total_profit DECIMAL(20,8) DEFAULT 0 COMMENT '总盈利',
    total_loss DECIMAL(20,8) DEFAULT 0 COMMENT '总亏损',
    net_profit DECIMAL(20,8) DEFAULT 0 COMMENT '净盈利',
    
    -- 交易统计
    total_trades INT DEFAULT 0 COMMENT '总交易数',
    winning_trades INT DEFAULT 0 COMMENT '盈利交易数',
    losing_trades INT DEFAULT 0 COMMENT '亏损交易数',
    win_rate DECIMAL(10,4) DEFAULT 0 COMMENT '胜率',
    
    -- 风险指标
    max_profit DECIMAL(20,8) DEFAULT 0 COMMENT '最大单笔盈利',
    max_loss DECIMAL(20,8) DEFAULT 0 COMMENT '最大单笔亏损',
    max_drawdown DECIMAL(20,8) DEFAULT 0 COMMENT '最大回撤',
    sharpe_ratio DECIMAL(10,4) DEFAULT 0 COMMENT '夏普比率',
    
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    
    UNIQUE KEY uk_uid_period (uid, period_type, period_start),
    INDEX idx_uid (uid),
    INDEX idx_period (period_type, period_start)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
COMMENT='用户盈利统计表';


-- ============================================================
-- 13. 用户排行榜设置表
-- ============================================================
CREATE TABLE IF NOT EXISTS user_leaderboard_settings (
    id INT AUTO_INCREMENT PRIMARY KEY,
    uid VARCHAR(64) UNIQUE NOT NULL COMMENT '用户ID',
    show_on_leaderboard TINYINT DEFAULT 1 COMMENT '是否显示在排行榜',
    display_name VARCHAR(64) COMMENT '显示名称',
    hide_profit_amount TINYINT DEFAULT 0 COMMENT '是否隐藏盈利金额',
    public_dashboard TINYINT DEFAULT 0 COMMENT '是否公开仪表盘',
    public_dashboard_token VARCHAR(128) COMMENT '公开仪表盘访问令牌',
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    
    INDEX idx_show (show_on_leaderboard)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
COMMENT='用户排行榜设置表';


-- ============================================================
-- 14. 系统设置表
-- ============================================================
CREATE TABLE IF NOT EXISTS system_settings (
    `key` VARCHAR(128) PRIMARY KEY COMMENT '设置键',
    value TEXT COMMENT '设置值',
    description TEXT COMMENT '描述',
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
COMMENT='系统设置表';


-- ============================================================
-- 初始化系统设置
-- ============================================================
INSERT INTO system_settings (`key`, value, description) VALUES
    ('registration_enabled', 'true', '是否开放注册'),
    ('invite_code_required', 'false', '注册是否需要邀请码'),
    ('max_users', '10000', '最大用户数'),
    ('maintenance_mode', 'false', '维护模式')
ON DUPLICATE KEY UPDATE updated_at = CURRENT_TIMESTAMP;


-- ============================================================
-- 初始化返佣比例 (15级)
-- ============================================================
INSERT INTO referral_commission_rates (level, rate, min_amount, is_enabled) VALUES
    (1, 0.100000, 0, 1),   -- 10%
    (2, 0.050000, 0, 1),   -- 5%
    (3, 0.030000, 0, 1),   -- 3%
    (4, 0.020000, 0, 1),   -- 2%
    (5, 0.010000, 0, 1),   -- 1%
    (6, 0.005000, 0, 1),   -- 0.5%
    (7, 0.005000, 0, 1),
    (8, 0.005000, 0, 1),
    (9, 0.005000, 0, 1),
    (10, 0.005000, 0, 1),
    (11, 0.003000, 0, 1),  -- 0.3%
    (12, 0.003000, 0, 1),
    (13, 0.003000, 0, 1),
    (14, 0.003000, 0, 1),
    (15, 0.003000, 0, 1)
ON DUPLICATE KEY UPDATE updated_at = CURRENT_TIMESTAMP;


SET FOREIGN_KEY_CHECKS = 1;

-- ============================================================
-- 完成
-- ============================================================
-- 
-- 表结构说明:
-- 
-- 1. users                    - 用户基本信息
-- 2. user_exchange_config     - 用户交易所配置 (多交易所)
-- 3. user_trading_config      - 用户交易配置 (全局)
-- 4. user_ai_strategies       - 用户AI策略 (v5.0)
-- 5. strategy_templates       - 系统预设策略模板
-- 6. ai_decisions             - AI决策历史
-- 7. user_referrals           - 用户推荐关系
-- 8. referral_commission_rates - 返佣比例配置
-- 9. referral_commissions     - 返佣记录
-- 10. user_commission_balance - 用户返佣余额
-- 11. commission_withdrawals  - 提现记录
-- 12. user_profit_stats       - 盈利统计
-- 13. user_leaderboard_settings - 排行榜设置
-- 14. system_settings         - 系统设置
-- 
-- v5.0 策略架构:
-- - user_ai_strategies.strategy_preset: 选择预设策略
-- - user_ai_strategies.strategy_overrides: 自定义覆盖特定分类
-- - strategy_templates: 系统预设策略模板 (7个分类)
--   - role: 角色定义
--   - risk_rules: 风险规则
--   - entry_conditions: 入场条件
--   - exit_conditions: 出场条件
--   - position_sizing: 仓位管理
--   - market_preferences: 市场偏好
--   - adaptive_rules: 适应规则
-- 
-- 插入预设策略数据:
--   python scripts/insert_strategy_presets.py
-- ============================================================
