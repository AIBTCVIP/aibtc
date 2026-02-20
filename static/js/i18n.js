/**
 * 轻量级国际化模块
 * 
 * 特性：
 * - 无外部依赖，纯 JavaScript 实现
 * - 支持嵌套 key（如 'nav.dashboard'）
 * - 支持参数替换（如 '{count} 条记录'）
 * - 自动检测浏览器语言
 * - 语言切换时自动保存到 localStorage
 * - 防止页面闪烁（FOUC）
 * 
 * 使用方法：
 * 1. 引入此脚本
 * 2. 调用 I18n.init() 初始化
 * 3. 使用 I18n.t('key') 获取翻译
 * 4. 使用 I18n.setLocale('en-US') 切换语言
 * 
 * 防闪烁：
 * - 在 <head> 中添加: <style>.i18n-loading { opacity: 0; }</style>
 * - 在 <body> 上添加: class="i18n-loading"
 * - I18n.init() 完成后会自动移除该 class
 */

// 防闪烁说明：
// .i18n-loading 和 .i18n-ready 样式已在 common.css 中定义（随 <link> 同步加载）。
// 不再通过 JS 动态注入 <style>，避免样式注入与 DOM 渲染的竞态条件。
// body 上的 i18n-loading class 在 HTML 中静态声明，确保首帧即隐藏。
(function() {
  // 标记是否已经显示内容
  window._i18nContentShown = false;
  
  // 安全超时：如果 3 秒内没有调用 showContent()，强制显示页面
  // 防止 i18n 加载失败、auth API 超时等情况导致永久白屏
  window._i18nSafetyTimer = setTimeout(function() {
    if (!window._i18nContentShown && document.body) {
      if (window.SafeLog) SafeLog.warn('[I18n] Safety timeout: forcing content display after 3s');
      document.body.classList.remove('i18n-loading');
      document.body.classList.add('i18n-ready');
      window._i18nContentShown = true;
    }
  }, 3000);
})();

const I18n = {
  // 当前语言
  locale: 'zh-CN',
  
  // 支持的语言列表
  supportedLocales: [
    { code: 'zh-CN', name: '简体中文', flag: '🇨🇳' },
    { code: 'zh-TW', name: '繁體中文', flag: '🇹🇼' },
    { code: 'en-US', name: 'English', flag: '🇺🇸' },
    { code: 'ja-JP', name: '日本語', flag: '🇯🇵' },
    { code: 'ko-KR', name: '한국어', flag: '🇰🇷' },
    { code: 'es-ES', name: 'Español', flag: '🇪🇸' },
    { code: 'pt-BR', name: 'Português', flag: '🇧🇷' },
    { code: 'ru-RU', name: 'Русский', flag: '🇷🇺' },
    { code: 'ar-SA', name: 'العربية', flag: '🇸🇦' },  // RTL disabled for now due to layout issues
  ],
  
  // 语言包缓存
  messages: {},
  
  // 语言包加载状态
  loaded: {},
  
  // 语言变更回调
  onLocaleChange: null,
  
  /**
   * 初始化
   * @param {Object} options - 配置选项
   * @param {string} options.defaultLocale - 默认语言
   * @param {Function} options.onLocaleChange - 语言变更回调
   */
  async init(options = {}) {
    // 设置回调
    if (options.onLocaleChange) {
      this.onLocaleChange = options.onLocaleChange;
    }
    
    // 确定初始语言（优先级：localStorage > 浏览器语言 > 默认）
    const savedLocale = localStorage.getItem('locale');
    const browserLocale = this.detectBrowserLocale();
    const defaultLocale = options.defaultLocale || 'zh-CN';
    
    this.locale = savedLocale || browserLocale || defaultLocale;
    
    // 验证语言是否支持
    if (!this.supportedLocales.find(l => l.code === this.locale)) {
      this.locale = defaultLocale;
    }
    
    // 加载语言包
    await this.loadLocale(this.locale);
    
    // 设置 RTL
    this.updateDocumentDirection();
    
    return this;
  },
  
  /**
   * 显示页面内容（移除 loading 状态）
   * 在 Vue 应用挂载完成后调用
   */
  showContent() {
    // 标记已显示，防止 DOMContentLoaded 事件再次添加 loading class
    window._i18nContentShown = true;
    
    // 清除安全超时计时器（正常流程已完成，不需要强制显示）
    if (window._i18nSafetyTimer) {
      clearTimeout(window._i18nSafetyTimer);
      window._i18nSafetyTimer = null;
    }
    
    if (document.body) {
      document.body.classList.remove('i18n-loading');
      document.body.classList.add('i18n-ready');
    }
  },
  
  /**
   * 检测浏览器语言
   */
  detectBrowserLocale() {
    const browserLang = navigator.language || navigator.userLanguage;
    
    // 精确匹配
    const exact = this.supportedLocales.find(l => l.code === browserLang);
    if (exact) return exact.code;
    
    // 语言代码匹配（如 zh 匹配 zh-CN）
    const langCode = browserLang.split('-')[0];
    const partial = this.supportedLocales.find(l => l.code.startsWith(langCode));
    if (partial) return partial.code;
    
    return null;
  },
  
  /**
   * 获取版本号（优先使用全局 APP_VERSION，否则使用内置版本）
   */
  getVersion() {
    return window.APP_VERSION || '20260207';
  },
  
  /**
   * 加载语言包
   */
  async loadLocale(locale) {
    if (this.loaded[locale]) return;
    
    try {
      // 动态加载语言包（使用集中版本号，允许浏览器缓存）
      const version = this.getVersion();
      const response = await fetch(`/static/locales/${locale}.json?v=${version}`);
      if (!response.ok) {
        throw new Error(`Failed to load locale: ${locale}`);
      }
      
      this.messages[locale] = await response.json();
      this.loaded[locale] = true;
    } catch (e) {
      if (window.SafeLog) SafeLog.error(`[I18n] Failed to load locale ${locale}:`, e);
      
      // 如果加载失败且不是默认语言，尝试加载默认语言
      if (locale !== 'zh-CN' && !this.loaded['zh-CN']) {
        await this.loadLocale('zh-CN');
      }
    }
  },
  
  /**
   * 切换语言
   */
  async setLocale(locale) {
    if (locale === this.locale) return;
    
    // 验证语言是否支持
    if (!this.supportedLocales.find(l => l.code === locale)) {
      if (window.SafeLog) SafeLog.warn(`[I18n] Unsupported locale: ${locale}`);
      return;
    }
    
    // 加载语言包
    await this.loadLocale(locale);
    
    // 更新当前语言
    this.locale = locale;
    localStorage.setItem('locale', locale);
    
    // 更新文档方向
    this.updateDocumentDirection();
    
    // 触发回调
    if (this.onLocaleChange) {
      this.onLocaleChange(locale);
    }
  },
  
  /**
   * 更新文档方向（RTL 支持）
   */
  updateDocumentDirection() {
    const localeInfo = this.supportedLocales.find(l => l.code === this.locale);
    const isRtl = localeInfo?.rtl || false;
    
    document.documentElement.dir = isRtl ? 'rtl' : 'ltr';
    document.documentElement.lang = this.locale;
  },
  
  /**
   * 获取翻译
   * @param {string} key - 翻译 key（支持嵌套，如 'nav.dashboard'）
   * @param {Object} params - 参数替换（如 {count: 10}）
   * @returns {string} 翻译后的文本
   */
  t(key, params = {}) {
    // 获取当前语言的消息
    let messages = this.messages[this.locale];
    
    // 如果当前语言没有加载，尝试使用默认语言
    if (!messages) {
      messages = this.messages['zh-CN'];
    }
    
    if (!messages) {
      return key; // 返回 key 作为 fallback
    }
    
    // 解析嵌套 key
    const keys = key.split('.');
    let value = messages;
    
    for (const k of keys) {
      if (value && typeof value === 'object' && k in value) {
        value = value[k];
      } else {
        // 如果找不到，尝试从默认语言获取
        if (this.locale !== 'zh-CN' && this.messages['zh-CN']) {
          return this.t_fallback(key, params);
        }
        return key; // 返回 key 作为 fallback
      }
    }
    
    // 如果值不是字符串，返回 key
    if (typeof value !== 'string') {
      return key;
    }
    
    // 参数替换
    return this.interpolate(value, params);
  },
  
  /**
   * 从默认语言获取翻译（fallback）
   */
  t_fallback(key, params = {}) {
    const messages = this.messages['zh-CN'];
    if (!messages) return key;
    
    const keys = key.split('.');
    let value = messages;
    
    for (const k of keys) {
      if (value && typeof value === 'object' && k in value) {
        value = value[k];
      } else {
        return key;
      }
    }
    
    if (typeof value !== 'string') return key;
    
    return this.interpolate(value, params);
  },
  
  /**
   * 参数替换
   * 支持 {name} 和 {0}, {1} 格式
   */
  interpolate(text, params) {
    if (!params || Object.keys(params).length === 0) {
      return text;
    }
    
    return text.replace(/\{(\w+)\}/g, (match, key) => {
      return Object.prototype.hasOwnProperty.call(params, key) ? params[key] : match;
    });
  },
  
  /**
   * 获取当前语言信息
   */
  getCurrentLocale() {
    return this.supportedLocales.find(l => l.code === this.locale) || this.supportedLocales[0];
  },
  
  /**
   * 格式化数字
   */
  formatNumber(value, options = {}) {
    try {
      return new Intl.NumberFormat(this.locale, options).format(value);
    } catch (e) {
      return String(value);
    }
  },
  
  /**
   * 格式化日期
   */
  formatDate(date, options = {}) {
    try {
      const d = date instanceof Date ? date : new Date(date);
      return new Intl.DateTimeFormat(this.locale, options).format(d);
    } catch (e) {
      return String(date);
    }
  },
  
  /**
   * 格式化相对时间（如 "3 分钟前"）
   */
  formatRelativeTime(date) {
    const now = Date.now();
    const d = date instanceof Date ? date.getTime() : new Date(date).getTime();
    const diff = now - d;
    
    const seconds = Math.floor(diff / 1000);
    const minutes = Math.floor(seconds / 60);
    const hours = Math.floor(minutes / 60);
    const days = Math.floor(hours / 24);
    
    if (days > 0) {
      return this.t('common.daysAgo', { count: days });
    } else if (hours > 0) {
      return this.t('common.hoursAgo', { count: hours });
    } else if (minutes > 0) {
      return this.t('common.minutesAgo', { count: minutes });
    } else {
      return this.t('common.justNow');
    }
  },
  
  /**
   * 更新页面 SEO 元标签
   * @param {string} pageKey - 页面 key（如 'index', 'login', 'dashboard'）
   */
  updateSeoMeta(pageKey) {
    const seo = this.messages[this.locale]?.seo;
    if (!seo) return;
    
    const page = seo.pages?.[pageKey];
    if (!page) return;
    
    // 更新 title
    if (page.title) {
      document.title = page.title;
    }
    
    // 更新 meta description
    const descMeta = document.querySelector('meta[name="description"]');
    if (descMeta && page.description) {
      descMeta.setAttribute('content', page.description);
    }
    
    // 更新 meta keywords
    const keywordsMeta = document.querySelector('meta[name="keywords"]');
    if (keywordsMeta && seo.keywords) {
      keywordsMeta.setAttribute('content', seo.keywords);
    }
    
    // 更新 Open Graph
    const ogTitle = document.querySelector('meta[property="og:title"]');
    if (ogTitle && page.title) {
      ogTitle.setAttribute('content', page.title);
    }
    
    const ogDesc = document.querySelector('meta[property="og:description"]');
    if (ogDesc && page.description) {
      ogDesc.setAttribute('content', page.description);
    }
    
    // 更新 Twitter
    const twTitle = document.querySelector('meta[property="twitter:title"]');
    if (twTitle && page.title) {
      twTitle.setAttribute('content', page.title);
    }
    
    const twDesc = document.querySelector('meta[property="twitter:description"]');
    if (twDesc && page.description) {
      twDesc.setAttribute('content', page.description);
    }
    
    // 更新 html lang 属性
    document.documentElement.lang = this.locale.split('-')[0];
  },
  
  /**
   * 获取 SEO 数据
   * @param {string} pageKey - 页面 key
   * @returns {Object} SEO 数据
   */
  getSeo(pageKey) {
    const seo = this.messages[this.locale]?.seo || {};
    const page = seo.pages?.[pageKey] || {};
    
    return {
      siteName: seo.siteName || 'AIBTC.VIP',
      siteSlogan: seo.siteSlogan || '',
      siteDescription: seo.siteDescription || '',
      keywords: seo.keywords || '',
      title: page.title || seo.siteName,
      description: page.description || seo.siteDescription,
      features: seo.features || {}
    };
  }
};

// 便捷函数
function t(key, params) {
  return I18n.t(key, params);
}

// 导出到全局
window.I18n = I18n;
window.t = t;
