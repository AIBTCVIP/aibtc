/**
 * 集中版本管理
 * 
 * 发布新版本时只需更新此文件中的 APP_VERSION
 * 所有页面会自动使用新版本号
 * 
 * 注意事项：
 * 1. version.js 本身不带版本号，依赖服务器缓存策略（建议设置 max-age=3600 或更短）
 * 2. auth.js, nav.js, i18n.js 等核心 JS 也不带版本号，同样依赖服务器缓存策略
 * 3. 只有 app.js 和语言包 JSON 使用动态版本号（通过 loadVersionedScript 和 I18n.getVersion）
 * 
 * 服务器缓存配置建议（Nginx）：
 * location /static/js/version.js {
 *     add_header Cache-Control "public, max-age=3600";  # 1小时
 * }
 * location ~* \.(js|css)$ {
 *     add_header Cache-Control "public, max-age=86400";  # 1天
 * }
 */
(function() {
  'use strict';
  
  // ========================================
  // 发布新版本时更新此值
  // ========================================
  const APP_VERSION = '20260210';
  
  // 导出到全局
  window.APP_VERSION = APP_VERSION;
  
  /**
   * 获取带版本号的资源 URL
   * @param {string} path - 资源路径，如 '/static/app.js'
   * @returns {string} 带版本号的 URL
   */
  window.getVersionedUrl = function(path) {
    const separator = path.includes('?') ? '&' : '?';
    return `${path}${separator}v=${APP_VERSION}`;
  };
  
  /**
   * 动态加载带版本号的脚本
   * @param {string} src - 脚本路径
   * @param {Object} options - 可选配置
   * @param {boolean} options.async - 是否异步加载
   * @param {boolean} options.defer - 是否延迟加载
   * @returns {Promise} 加载完成的 Promise
   */
  window.loadVersionedScript = function(src, options = {}) {
    return new Promise((resolve, reject) => {
      const script = document.createElement('script');
      script.src = getVersionedUrl(src);
      if (options.async) script.async = true;
      if (options.defer) script.defer = true;
      script.onload = resolve;
      script.onerror = reject;
      document.body.appendChild(script);
    });
  };
  
  /**
   * 预加载资源
   * @param {string} href - 资源路径
   * @param {string} as - 资源类型 ('script', 'style', 'fetch', 'image')
   * @param {boolean} versioned - 是否添加版本号
   */
  function preloadResource(href, as, versioned = false) {
    const link = document.createElement('link');
    link.rel = 'preload';
    link.href = versioned ? getVersionedUrl(href) : href;
    link.as = as;
    if (as === 'fetch') {
      link.crossOrigin = 'anonymous';
    }
    document.head.appendChild(link);
  }
  
  /**
   * 智能预加载语言包
   * 根据用户保存的语言偏好或浏览器语言预加载对应的语言包
   */
  window.preloadLocale = function() {
    // 支持的语言列表
    const supportedLocales = [
      'zh-CN', 'zh-TW', 'en-US', 'ja-JP', 'ko-KR',
      'es-ES', 'pt-BR', 'ru-RU', 'ar-SA'
    ];
    
    // 确定要预加载的语言
    let locale = localStorage.getItem('locale');
    
    if (!locale) {
      // 从浏览器语言检测
      const browserLang = navigator.language || navigator.userLanguage || '';
      
      // 精确匹配
      if (supportedLocales.includes(browserLang)) {
        locale = browserLang;
      } else {
        // 前缀匹配（如 zh -> zh-CN, en -> en-US）
        const langPrefix = browserLang.split('-')[0].toLowerCase();
        const prefixMap = {
          'zh': 'zh-CN',
          'en': 'en-US',
          'ja': 'ja-JP',
          'ko': 'ko-KR',
          'es': 'es-ES',
          'pt': 'pt-BR',
          'ru': 'ru-RU',
          'ar': 'ar-SA'
        };
        locale = prefixMap[langPrefix] || 'zh-CN';
      }
    }
    
    // 验证语言是否支持
    if (!supportedLocales.includes(locale)) {
      locale = 'zh-CN';
    }
    
    // 预加载语言包（带版本号）
    preloadResource(`/static/locales/${locale}.json`, 'fetch', true);
    
    return locale;
  };
  
  // 自动执行预加载
  window.preloadLocale();
})();
