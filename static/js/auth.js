/**
 * 认证工具模块
 * 
 * 安全特性:
 * - 优先使用 HttpOnly Cookie 认证（由服务器设置）
 * - localStorage 仅存储非敏感的用户信息（uid, username）用于显示
 * - Token 不再暴露到全局变量
 * - CSRF 保护：对 POST/PUT/DELETE 请求自动添加 CSRF Token
 * - 生产环境禁用 console 输出
 * - API 请求超时保护
 */

// CSRF Cookie 名称（与后端一致）
const CSRF_COOKIE_NAME = 'csrf_token';
const CSRF_HEADER_NAME = 'X-CSRF-Token';

// 默认请求超时时间（毫秒）
const DEFAULT_TIMEOUT = 30000;

/**
 * 安全日志工具
 * 生产环境（非 localhost）禁用 console 输出，防止信息泄露
 */
const SafeLog = {
    _isProduction: !['localhost', '127.0.0.1'].includes(window.location.hostname),
    
    log(...args) {
        if (!this._isProduction) {
            console.log(...args);
        }
    },
    
    warn(...args) {
        if (!this._isProduction) {
            console.warn(...args);
        }
    },
    
    error(...args) {
        if (!this._isProduction) {
            console.error(...args);
        }
    },
    
    // 强制输出（仅用于关键错误，生产环境也会显示）
    critical(...args) {
        console.error('[CRITICAL]', ...args);
    }
};

// 导出到全局
window.SafeLog = SafeLog;

/**
 * 从 Cookie 中获取 CSRF Token
 */
function getCsrfToken() {
    const cookies = document.cookie.split(';');
    for (let cookie of cookies) {
        const [name, value] = cookie.trim().split('=');
        if (name === CSRF_COOKIE_NAME) {
            return decodeURIComponent(value);
        }
    }
    return null;
}

const Auth = {
    /**
     * 获取 CSRF Token（供外部使用）
     */
    getCsrfToken() {
        return getCsrfToken();
    },
    
    /**
     * 获取当前用户信息（从 sessionStorage 读取非敏感信息）
     * 注意：实际的认证通过 HttpOnly Cookie 完成
     * 使用 sessionStorage 而非 localStorage，会话结束后自动清除，更安全
     */
    getUser() {
        // 优先从 sessionStorage 读取
        let uid = sessionStorage.getItem('uid');
        let username = sessionStorage.getItem('username');
        
        // 兼容：如果 sessionStorage 没有，尝试从 localStorage 迁移
        if (!uid) {
            uid = localStorage.getItem('uid');
            username = localStorage.getItem('username');
            if (uid) {
                // 迁移到 sessionStorage
                sessionStorage.setItem('uid', uid);
                sessionStorage.setItem('username', username || '');
                // 清除 localStorage 中的数据
                localStorage.removeItem('uid');
                localStorage.removeItem('username');
            }
        }
        
        if (uid) {
            return { uid, username: username || '' };
        }
        return null;
    },

    /**
     * 保存用户信息（仅非敏感数据）
     * Token 由服务器通过 HttpOnly Cookie 设置
     * 使用 sessionStorage，会话结束后自动清除
     */
    saveUser(uid, username) {
        sessionStorage.setItem('uid', uid);
        sessionStorage.setItem('username', username);
        // 清除可能存在的旧 localStorage 数据
        localStorage.removeItem('uid');
        localStorage.removeItem('username');
    },

    /**
     * 清除用户信息
     */
    clearUser() {
        sessionStorage.removeItem('uid');
        sessionStorage.removeItem('username');
        // 清除可能存在的旧 localStorage 数据
        localStorage.removeItem('uid');
        localStorage.removeItem('username');
        localStorage.removeItem('token'); // 清除可能存在的旧 token
    },

    /**
     * 检查是否已登录
     * 通过调用需要认证的 API 来验证 Cookie 或旧 Token
     * 如果 Cookie 有效但 sessionStorage 为空，会自动恢复用户信息
     */
    async checkAuth() {
        try {
            const options = {
                credentials: 'include' // 携带 Cookie
            };
            
            // ============================================================
            // TODO: 旧 Token 兼容代码 - 计划移除日期: 2026-03-01
            // 
            // 此代码用于向后兼容从 localStorage 读取的旧 token。
            // 新用户已使用 HttpOnly Cookie 认证，此代码仅为迁移期保留。
            // 
            // 移除条件：
            // 1. 所有活跃用户已迁移到 Cookie 认证
            // 2. 距离 Cookie 认证上线已超过 30 天
            // 
            // 移除时需同步修改：
            // - checkAuth() 中的 oldToken 逻辑
            // - apiRequest() 中的 oldToken 逻辑
            // - clearUser() 中的 localStorage.removeItem('token')
            // ============================================================
            const oldToken = localStorage.getItem('token');
            if (oldToken) {
                options.headers = {
                    'Authorization': `Bearer ${oldToken}`
                };
            }
            
            const resp = await fetch('/api/user/config', options);
            if (resp.ok) {
                // Cookie 有效，检查是否需要恢复 sessionStorage
                const currentUser = this.getUser();
                if (!currentUser || !currentUser.uid) {
                    // sessionStorage 为空，从 API 响应恢复用户信息
                    try {
                        const data = await resp.json();
                        if (data.uid && data.username) {
                            this.saveUser(data.uid, data.username);
                            SafeLog.log('User info restored from API');
                        }
                    } catch (e) {
                        SafeLog.warn('Failed to restore user info:', e);
                    }
                }
                return true;
            }
            return false;
        } catch {
            return false;
        }
    },

    /**
     * 登出
     */
    async logout() {
        try {
            // 使用 apiRequest 以自动携带 CSRF Token
            await this.apiRequest('/api/user/logout', {
                method: 'POST'
            });
        } catch (e) {
            SafeLog.error('Logout error:', e);
        }
        this.clearUser();
        this.clearAdminCache();
        window.location.href = '/login.html';
    },

    /**
     * API 请求封装（自动携带 Cookie 和 CSRF Token）
     * @param {string} url - 请求 URL
     * @param {Object} options - fetch 选项
     * @param {number} timeout - 超时时间（毫秒），默认 30000
     */
    async apiRequest(url, options = {}, timeout = DEFAULT_TIMEOUT) {
        // 创建 AbortController 用于超时控制
        const controller = new AbortController();
        const timeoutId = setTimeout(() => controller.abort(), timeout);
        
        const defaultOptions = {
            credentials: 'include', // 关键：携带 HttpOnly Cookie
            signal: controller.signal,
            headers: {
                'Content-Type': 'application/json',
                ...options.headers
            }
        };

        // TODO: 旧 Token 兼容代码 - 计划移除日期: 2026-03-01（见 checkAuth 注释）
        const oldToken = localStorage.getItem('token');
        if (oldToken) {
            defaultOptions.headers['Authorization'] = `Bearer ${oldToken}`;
        }
        
        // 对于修改操作，添加 CSRF Token
        const method = (options.method || 'GET').toUpperCase();
        if (['POST', 'PUT', 'DELETE', 'PATCH'].includes(method)) {
            const csrfToken = getCsrfToken();
            if (csrfToken) {
                defaultOptions.headers[CSRF_HEADER_NAME] = csrfToken;
            }
        }

        try {
            // Merge options but ensure our timeout signal takes precedence
            const mergedOptions = { ...defaultOptions, ...options };
            mergedOptions.signal = controller.signal; // Always use our timeout-controlled signal
            const response = await fetch(url, mergedOptions);
            
            clearTimeout(timeoutId);

            if (response.status === 401) {
                // 认证失败，清除本地数据并跳转登录
                this.clearUser();
                window.location.href = '/login.html';
                const msg = typeof I18n !== 'undefined' ? I18n.t('error.authExpired') : 'Authentication expired, please login again';
                throw new Error(msg);
            }
            
            if (response.status === 403) {
                // 权限被拒绝，使用通用错误信息避免泄露实现细节
                const msg = typeof I18n !== 'undefined' ? I18n.t('error.requestDenied') : 'Request denied, please refresh and try again';
                throw new Error(msg);
            }

            return response;
        } catch (e) {
            clearTimeout(timeoutId);
            
            // 处理超时错误
            if (e.name === 'AbortError') {
                const msg = typeof I18n !== 'undefined' ? I18n.t('error.requestTimeout') : 'Request timeout, please try again';
                throw new Error(msg);
            }
            
            throw e;
        }
    },

    /**
     * GET 请求
     */
    async get(url) {
        return this.apiRequest(url);
    },

    /**
     * POST 请求
     */
    async post(url, data) {
        return this.apiRequest(url, {
            method: 'POST',
            body: JSON.stringify(data)
        });
    },

    /**
     * PUT 请求
     */
    async put(url, data) {
        return this.apiRequest(url, {
            method: 'PUT',
            body: JSON.stringify(data)
        });
    },

    /**
     * DELETE 请求
     */
    async delete(url) {
        return this.apiRequest(url, {
            method: 'DELETE'
        });
    },

    /**
     * 检查当前用户是否是管理员
     * 结果会缓存到 sessionStorage 避免重复请求
     * @returns {Promise<boolean>}
     */
    async isAdmin() {
        // 先检查缓存
        const cached = sessionStorage.getItem('isAdmin');
        if (cached !== null) {
            return cached === 'true';
        }

        try {
            const resp = await this.apiRequest('/api/admin/verify');
            if (resp.ok) {
                sessionStorage.setItem('isAdmin', 'true');
                return true;
            }
        } catch (e) {
            // 403 或其他错误表示非管理员
        }
        
        sessionStorage.setItem('isAdmin', 'false');
        return false;
    },

    /**
     * 清除管理员缓存（登出时调用）
     */
    clearAdminCache() {
        sessionStorage.removeItem('isAdmin');
    }
};

// 导出到全局（不包含 token）
window.Auth = Auth;
