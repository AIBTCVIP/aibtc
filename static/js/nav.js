/**
 * 共享导航组件
 * 
 * 使用方法：
 * 1. 在 HTML 中添加 <div id="shared-nav"></div>
 * 2. 引入此脚本 <script src="/static/js/nav.js"></script>
 * 3. 调用 initSharedNav({ currentPage: 'dashboard' })
 * 
 * 配置选项：
 * - currentPage: 当前页面标识 ('dashboard' | 'logic' | 'settings' | 'referral' | 'leaderboard' | 'admin')
 * - isPublicMode: 是否公开模式（隐藏设置、邀请等）
 * - publicDisplayName: 公开模式显示名称
 * - showPageTitle: 是否显示页面标题（如"用户设置"）
 * - pageTitle: 页面标题文字
 * - checkAdmin: 是否检查管理员权限并显示管理链接（默认 true）
 */

// 注入下拉菜单样式（确保在 Tailwind 加载前下拉菜单是隐藏的）
(function() {
  const style = document.createElement('style');
  style.id = 'nav-dropdown-style';
  style.textContent = `
    .nav-dropdown { opacity: 0; visibility: hidden; }
    .group:hover > .nav-dropdown { opacity: 1; visibility: visible; }
  `;
  if (document.head) {
    document.head.appendChild(style);
  } else {
    document.addEventListener('DOMContentLoaded', function() {
      if (!document.getElementById('nav-dropdown-style')) {
        document.head.appendChild(style);
      }
    }, { once: true });
  }
})();

const SharedNav = {
  // HTML 转义函数，防止 XSS 攻击
  // P2 Fix: 使用纯字符串替换，避免 DOM-based escaping 的潜在边缘情况
  escapeHtml(str) {
    if (!str) return '';
    return String(str)
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;')
      .replace(/'/g, '&#39;');
  },
  
  // 获取翻译文本（如果 I18n 可用）
  t(key, params) {
    if (typeof I18n !== 'undefined' && I18n.t) {
      return I18n.t(key, params);
    }
    // Fallback: 返回 key 的最后一部分
    return key.split('.').pop();
  },

  // 导航项配置 - 使用 i18n key
  navItems: {
    main: [
      { id: 'logic', labelKey: 'nav.aiDecision', href: '/#logic', icon: 'brain' },
      { id: 'dashboard', labelKey: 'nav.dashboard', href: '/#dashboard', icon: 'chart' },
    ],
    external: [
      {
        id: 'rebate',
        labelKey: 'nav.rebate',
        icon: 'dollar',
        color: 'emerald',
        dropdown: [
          { label: 'Binance', href: 'https://accounts.binance.com/register?ref=1162334440', icon: 'B', color: 'yellow' },
          { label: 'OKX', href: 'https://www.okx.com/join/88536686', icon: 'O', color: 'white' },
          { label: 'Hyperliquid', href: 'https://app.hyperliquid.xyz/join/AIBTC', icon: 'H', color: 'green' },
          { label: 'Bitget', href: 'https://partner.hdmune.cn/bg/J13A5U', icon: 'B', color: 'cyan' },
        ]
      },
      { id: 'ai500', labelKey: 'nav.smartPick', href: 'https://token.aibtc.vip/latest', icon: null },
      { id: 'aiproxy', labelKey: 'nav.aiProxy', href: 'https://api.aibtc.vip/register?aff=hjJ7', icon: 'dollar', color: 'amber', badgeKey: 'nav.discount' },
    ],
    social: [
      { id: 'telegram', href: 'https://t.me/aibtcchina', icon: 'telegram', title: 'Telegram' },
      { id: 'twitter', href: 'https://x.com/Aibtcvip', icon: 'twitter', title: 'X/Twitter' },
      { id: 'youtube', href: 'https://www.youtube.com/@AIBTCVIP', icon: 'youtube', title: 'YouTube' },
    ],
    user: [
      { id: 'statistics', labelKey: 'nav.statistics', href: '/statistics.html', requireAuth: true },
      { id: 'leaderboard', labelKey: 'nav.leaderboard', href: '/leaderboard.html', requireAuth: false },
      { id: 'referral', labelKey: 'nav.referral', href: '/referral.html', color: 'amber', requireAuth: true },
      { id: 'settings', labelKey: 'nav.settings', href: '/settings.html', requireAuth: true },
    ]
  },

  // SVG 图标
  icons: {
    brain: '<path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9.663 17h4.673M12 3v1m6.364 1.636l-.707.707M21 12h-1M4 12H3m3.343-5.657l-.707-.707m2.828 9.9a5 5 0 117.072 0l-.548.547A3.374 3.374 0 0014 18.469V19a2 2 0 11-4 0v-.531c0-.895-.356-1.754-.988-2.386l-.548-.547z"/>',
    chart: '<path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 19v-6a2 2 0 00-2-2H5a2 2 0 00-2 2v6a2 2 0 002 2h2a2 2 0 002-2zm0 0V9a2 2 0 012-2h2a2 2 0 012 2v10m-6 0a2 2 0 002 2h2a2 2 0 002-2m0 0V5a2 2 0 012-2h2a2 2 0 012 2v14a2 2 0 01-2 2h-2a2 2 0 01-2-2z"/>',
    dollar: '<path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 8c-1.657 0-3 .895-3 2s1.343 2 3 2 3 .895 3 2-1.343 2-3 2m0-8c1.11 0 2.08.402 2.599 1M12 8V7m0 1v8m0 0v1m0-1c-1.11 0-2.08-.402-2.599-1M21 12a9 9 0 11-18 0 9 9 0 0118 0z"/>',
    users: '<path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M17 20h5v-2a3 3 0 00-5.356-1.857M17 20H7m10 0v-2c0-.656-.126-1.283-.356-1.857M7 20H2v-2a3 3 0 015.356-1.857M7 20v-2c0-.656.126-1.283.356-1.857m0 0a5.002 5.002 0 019.288 0M15 7a3 3 0 11-6 0 3 3 0 016 0zm6 3a2 2 0 11-4 0 2 2 0 014 0zM7 10a2 2 0 11-4 0 2 2 0 014 0z"/>',
    settings: '<path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M10.325 4.317c.426-1.756 2.924-1.756 3.35 0a1.724 1.724 0 002.573 1.066c1.543-.94 3.31.826 2.37 2.37a1.724 1.724 0 001.065 2.572c1.756.426 1.756 2.924 0 3.35a1.724 1.724 0 00-1.066 2.573c.94 1.543-.826 3.31-2.37 2.37a1.724 1.724 0 00-2.572 1.065c-.426 1.756-2.924 1.756-3.35 0a1.724 1.724 0 00-2.573-1.066c-1.543.94-3.31-.826-2.37-2.37a1.724 1.724 0 00-1.065-2.572c-1.756-.426-1.756-2.924 0-3.35a1.724 1.724 0 001.066-2.573c-.94-1.543.826-3.31 2.37-2.37.996.608 2.296.07 2.572-1.065z"/><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M15 12a3 3 0 11-6 0 3 3 0 016 0z"/>',
    logout: '<path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M17 16l4-4m0 0l-4-4m4 4H7m6 4v1a3 3 0 01-3 3H6a3 3 0 01-3-3V7a3 3 0 013-3h4a3 3 0 013 3v1"/>',
    menu: '<path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M4 6h16M4 12h16M4 18h16"/>',
    close: '<path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M6 18L18 6M6 6l12 12"/>',
    chevronDown: '<path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M19 9l-7 7-7-7"/>',
    telegram: '<path d="M12 0C5.373 0 0 5.373 0 12s5.373 12 12 12 12-5.373 12-12S18.627 0 12 0zm5.562 8.161c-.18 1.897-.962 6.502-1.359 8.627-.168.9-.5 1.201-.82 1.23-.697.064-1.226-.461-1.901-.903-1.056-.693-1.653-1.124-2.678-1.8-1.185-.78-.417-1.21.258-1.91.177-.184 3.247-2.977 3.307-3.23.007-.032.014-.15-.056-.212s-.174-.041-.249-.024c-.106.024-1.793 1.139-5.062 3.345-.479.329-.913.489-1.302.481-.428-.009-1.252-.242-1.865-.442-.751-.244-1.349-.374-1.297-.789.027-.216.324-.437.893-.663 3.498-1.524 5.831-2.529 6.998-3.015 3.333-1.386 4.025-1.627 4.477-1.635.099-.002.321.023.465.141.121.1.155.234.17.33.015.098.033.32.018.493z"/>',
    twitter: '<path d="M18.244 2.25h3.308l-7.227 8.26 8.502 11.24H16.17l-5.214-6.817L4.99 21.75H1.68l7.73-8.835L1.254 2.25H8.08l4.713 6.231zm-1.161 17.52h1.833L7.084 4.126H5.117z"/>',
    youtube: '<path d="M23.498 6.186a3.016 3.016 0 0 0-2.122-2.136C19.505 3.545 12 3.545 12 3.545s-7.505 0-9.377.505A3.017 3.017 0 0 0 .502 6.186C0 8.07 0 12 0 12s0 3.93.502 5.814a3.016 3.016 0 0 0 2.122 2.136c1.871.505 9.376.505 9.376.505s7.505 0 9.377-.505a3.015 3.015 0 0 0 2.122-2.136C24 15.93 24 12 24 12s0-3.93-.502-5.814zM9.545 15.568V8.432L15.818 12l-6.273 3.568z"/>',
    admin: '<path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 12l2 2 4-4m5.618-4.016A11.955 11.955 0 0112 2.944a11.955 11.955 0 01-8.618 3.04A12.02 12.02 0 003 9c0 5.591 3.824 10.29 9 11.622 5.176-1.332 9-6.03 9-11.622 0-1.042-.133-2.052-.382-3.016z"/>',
    globe: '<path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M21 12a9 9 0 01-9 9m9-9a9 9 0 00-9-9m9 9H3m9 9a9 9 0 01-9-9m9 9c1.657 0 3-4.03 3-9s-1.343-9-3-9m0 18c-1.657 0-3-4.03-3-9s1.343-9 3-9m-9 9a9 9 0 019-9"/>',
  },
  
  // 获取语言切换器 HTML
  getLanguageSwitcherHTML() {
    if (typeof I18n === 'undefined') return '';
    
    const currentLocale = I18n.getCurrentLocale();
    const locales = I18n.supportedLocales;
    
    return `
      <div class="relative group">
        <button class="px-2 py-1.5 rounded-lg text-sm text-slate-400 hover:text-white hover:bg-slate-800/50 transition-all duration-200 flex items-center gap-1">
          <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">${this.icons.globe}</svg>
          <span class="hidden sm:inline">${currentLocale.flag || ''} ${currentLocale.code.split('-')[0].toUpperCase()}</span>
          <svg class="w-3 h-3 transition-transform group-hover:rotate-180" fill="none" stroke="currentColor" viewBox="0 0 24 24">${this.icons.chevronDown}</svg>
        </button>
        <div class="nav-dropdown absolute top-full right-0 mt-1 w-40 py-1.5 bg-slate-800/95 backdrop-blur-xl border border-slate-700/50 rounded-xl shadow-xl transition-all duration-200 z-50 max-h-80 overflow-y-auto">
          ${locales.map(locale => `
            <button onclick="SharedNav.setLocale('${locale.code}')"
               class="flex items-center gap-2 px-3 py-2 text-sm w-full text-left transition-colors
                      ${locale.code === currentLocale.code 
                        ? 'bg-blue-500/20 text-blue-400' 
                        : 'text-slate-300 hover:text-white hover:bg-slate-700/50'}">
              <span class="text-base">${locale.flag || ''}</span>
              <span>${locale.name}</span>
              ${locale.code === currentLocale.code ? '<svg class="w-4 h-4 ml-auto" fill="currentColor" viewBox="0 0 20 20"><path fill-rule="evenodd" d="M16.707 5.293a1 1 0 010 1.414l-8 8a1 1 0 01-1.414 0l-4-4a1 1 0 011.414-1.414L8 12.586l7.293-7.293a1 1 0 011.414 0z" clip-rule="evenodd"/></svg>' : ''}
            </button>
          `).join('')}
        </div>
      </div>
    `;
  },
  
  // 切换语言
  setLocale(locale) {
    if (typeof I18n !== 'undefined' && I18n.setLocale) {
      I18n.setLocale(locale);
    }
  },

  // 当前配置
  config: {
    currentPage: 'dashboard',
    isPublicMode: false,
    publicDisplayName: '',
    showPageTitle: false,
    pageTitle: '',
    mobileMenuOpen: false,
    checkAdmin: true,
    isAdmin: false,
    // 主页模式：页面内切换而不是跳转
    isHomePage: false,
    onPageChange: null, // 页面切换回调函数 (page) => void
  },
  
  // 事件监听器引用（用于清理）
  _eventListeners: [],
  
  // Tailwind 颜色类映射（避免动态类名不被编译）
  colorClasses: {
    yellow: { bg: 'bg-yellow-500/20', text: 'text-yellow-400' },
    white: { bg: 'bg-white/10', text: 'text-white' },
    green: { bg: 'bg-green-500/20', text: 'text-green-400' },
    cyan: { bg: 'bg-cyan-500/20', text: 'text-cyan-400' },
    amber: { bg: 'bg-amber-500/20', text: 'text-amber-400' },
    blue: { bg: 'bg-blue-500/20', text: 'text-blue-400' },
    purple: { bg: 'bg-purple-500/20', text: 'text-purple-400' },
    red: { bg: 'bg-red-500/20', text: 'text-red-400' },
    emerald: { bg: 'bg-emerald-500/20', text: 'text-emerald-400' },
  },
  
  // 获取颜色类（安全方式，避免动态类名）
  getColorClasses(color) {
    return this.colorClasses[color] || this.colorClasses.blue;
  },

  // 初始化
  async init(options = {}) {
    this.config = { ...this.config, ...options };
    
    // 检查管理员权限
    if (this.config.checkAdmin && !this.config.isPublicMode && typeof Auth !== 'undefined') {
      try {
        this.config.isAdmin = await Auth.isAdmin();
      } catch (e) {
        this.config.isAdmin = false;
      }
    }
    
    this.render();
    this.bindEvents();
  },
  
  // 更新当前页面（用于主页模式）
  setCurrentPage(page) {
    this.config.currentPage = page;
    this.render();
    this.bindEvents();
  },

  // 渲染导航
  render() {
    const container = document.getElementById('shared-nav');
    if (!container) return;

    container.innerHTML = this.getNavHTML();
  },

  // 获取导航 HTML
  getNavHTML() {
    const { currentPage, isPublicMode, publicDisplayName, showPageTitle, pageTitle } = this.config;
    // 转义用户可控的内容，防止 XSS
    const safeDisplayName = this.escapeHtml(publicDisplayName);
    const safePageTitle = this.escapeHtml(pageTitle);

    return `
    <div class="sticky top-0 z-50 backdrop-blur-xl bg-slate-900/80 border-b border-slate-700/50 shadow-xl shadow-black/10">
      <div class="max-w-7xl mx-auto px-4">
        <div class="h-16 flex items-center justify-between">
          <!-- Logo -->
          <div class="flex items-center gap-3">
            <a href="/" class="relative">
              <div class="text-xl font-bold bg-gradient-to-r from-blue-400 via-purple-400 to-pink-400 bg-clip-text text-transparent">
                AIBTC.VIP
              </div>
              <div class="absolute -bottom-1 left-0 right-0 h-0.5 bg-gradient-to-r from-blue-400 via-purple-400 to-pink-400 rounded-full opacity-50"></div>
            </a>
            ${isPublicMode ? `
              <span class="inline-flex items-center px-2 py-0.5 rounded-full text-[10px] font-medium bg-green-500/20 text-green-300 border border-green-500/30">
                PUBLIC
              </span>
              ${safeDisplayName ? `<span class="hidden sm:inline text-slate-400 text-sm">${safeDisplayName}</span>` : ''}
            ` : `
              <span class="hidden sm:inline-flex items-center px-2 py-0.5 rounded-full text-[10px] font-medium bg-blue-500/20 text-blue-300 border border-blue-500/30">
                BETA V10
              </span>
            `}
            ${showPageTitle && safePageTitle ? `
              <div class="hidden md:block h-6 w-px bg-slate-700"></div>
              <span class="hidden md:block text-slate-400 text-sm">${safePageTitle}</span>
            ` : ''}
          </div>

          <!-- Desktop Navigation -->
          <nav class="hidden lg:flex items-center gap-1">
            ${this.getDesktopNavHTML()}
          </nav>

          <!-- Mobile Menu Button -->
          <button id="nav-mobile-toggle" class="lg:hidden p-2 rounded-lg hover:bg-slate-800 transition-colors text-white">
            <svg id="nav-menu-icon" class="w-6 h-6" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              ${this.icons.menu}
            </svg>
            <svg id="nav-close-icon" class="w-6 h-6 hidden" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              ${this.icons.close}
            </svg>
          </button>
        </div>

        <!-- Mobile Menu -->
        <div id="nav-mobile-menu" class="hidden lg:hidden py-3 border-t border-slate-700/50 max-h-[calc(100vh-4rem)] overflow-y-auto">
          ${this.getMobileNavHTML()}
        </div>
      </div>
    </div>
    `;
  },

  // 桌面端导航 HTML
  getDesktopNavHTML() {
    const { currentPage, isPublicMode, isHomePage } = this.config;
    let html = '';

    // 主导航 - 紧凑样式
    this.navItems.main.forEach(item => {
      // 公开模式下隐藏 AI 决策页面
      if (isPublicMode && item.id === 'logic') return;
      
      const isActive = currentPage === item.id;
      const label = item.labelKey ? this.t(item.labelKey) : item.label;
      // 主页模式使用 onclick 切换，其他页面使用 href 跳转
      if (isHomePage) {
        html += `
          <a href="#${item.id}" data-nav-page="${item.id}"
             class="px-3 py-1.5 rounded-lg text-sm font-medium transition-all duration-200 border
                    ${isActive 
                      ? 'bg-blue-500/20 text-blue-400 border-blue-500/30' 
                      : 'text-slate-400 hover:text-white hover:bg-slate-800/50 border-transparent'}">
            ${label}
          </a>
        `;
      } else {
        html += `
          <a href="${item.href}"
             class="px-3 py-1.5 rounded-lg text-sm font-medium transition-all duration-200 border
                    ${isActive 
                      ? 'bg-blue-500/20 text-blue-400 border-blue-500/30' 
                      : 'text-slate-400 hover:text-white hover:bg-slate-800/50 border-transparent'}">
            ${label}
          </a>
        `;
      }
    });

    html += '<div class="w-px h-5 bg-slate-700 mx-1"></div>';

    // 返佣下拉菜单
    const rebateItem = this.navItems.external.find(i => i.id === 'rebate');
    if (rebateItem) {
      html += `
        <div class="relative group">
          <button class="px-2 py-1.5 rounded-lg text-sm text-emerald-400 hover:text-emerald-300 hover:bg-emerald-500/10 transition-all duration-200 flex items-center gap-1">
            <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">${this.icons.dollar}</svg>
            ${this.t('nav.rebate')}
            <svg class="w-3 h-3 transition-transform group-hover:rotate-180" fill="none" stroke="currentColor" viewBox="0 0 24 24">${this.icons.chevronDown}</svg>
          </button>
          <div class="nav-dropdown absolute top-full left-0 mt-1 w-40 py-1.5 bg-slate-800/95 backdrop-blur-xl border border-slate-700/50 rounded-xl shadow-xl transition-all duration-200 z-50">
            ${rebateItem.dropdown.map(sub => {
              const colors = this.getColorClasses(sub.color);
              return `
              <a href="${sub.href}" target="_blank" rel="noopener noreferrer"
                 class="flex items-center gap-2 px-3 py-2 text-sm text-slate-300 hover:text-white hover:bg-slate-700/50 transition-colors">
                <span class="w-5 h-5 rounded-full ${colors.bg} flex items-center justify-center ${colors.text} text-xs font-bold">${sub.icon}</span>
                ${sub.label}
              </a>
            `}).join('')}
          </div>
        </div>
      `;
    }

    // 更多下拉菜单（智能选币、AI中转站）
    html += `
      <div class="relative group">
        <button class="px-2 py-1.5 rounded-lg text-sm text-slate-400 hover:text-white hover:bg-slate-800/50 transition-all duration-200 flex items-center gap-1">
          ${this.t('nav.more')}
          <svg class="w-3 h-3 transition-transform group-hover:rotate-180" fill="none" stroke="currentColor" viewBox="0 0 24 24">${this.icons.chevronDown}</svg>
        </button>
        <div class="nav-dropdown absolute top-full left-0 mt-1 w-40 py-1.5 bg-slate-800/95 backdrop-blur-xl border border-slate-700/50 rounded-xl shadow-xl transition-all duration-200 z-50">
          <a href="https://token.aibtc.vip/latest" target="_blank" rel="noopener noreferrer"
             class="flex items-center gap-2 px-3 py-2 text-sm text-slate-300 hover:text-white hover:bg-slate-700/50 transition-colors">
            <svg class="w-4 h-4 text-purple-400" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M13 10V3L4 14h7v7l9-11h-7z"/></svg>
            ${this.t('nav.smartPick')}
          </a>
          <a href="https://api.aibtc.vip/register?aff=hjJ7" target="_blank" rel="noopener noreferrer"
             class="flex items-center gap-2 px-3 py-2 text-sm text-amber-400 hover:text-amber-300 hover:bg-slate-700/50 transition-colors">
            <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M8 9l3 3-3 3m5 0h3M5 20h14a2 2 0 002-2V6a2 2 0 00-2-2H5a2 2 0 00-2 2v12a2 2 0 002 2z"/></svg>
            ${this.t('nav.aiProxy')}
            <span class="text-[10px] bg-amber-500/20 text-amber-300 px-1 py-0.5 rounded ml-auto">${this.t('nav.discount')}</span>
          </a>
        </div>
      </div>
    `;

    html += '<div class="w-px h-5 bg-slate-700 mx-1"></div>';

    // 社交图标
    html += `
      <a href="https://t.me/aibtcchina" target="_blank" rel="noopener noreferrer"
         class="p-1.5 rounded-lg text-slate-400 hover:text-blue-400 hover:bg-slate-800/50 transition-all duration-200" title="Telegram" aria-label="Telegram">
        <svg class="w-4 h-4" fill="currentColor" viewBox="0 0 24 24">${this.icons.telegram}</svg>
      </a>
      <a href="https://x.com/Aibtcvip" target="_blank" rel="noopener noreferrer"
         class="p-1.5 rounded-lg text-slate-400 hover:text-white hover:bg-slate-800/50 transition-all duration-200" title="X" aria-label="X/Twitter">
        <svg class="w-4 h-4" fill="currentColor" viewBox="0 0 24 24">${this.icons.twitter}</svg>
      </a>
      <a href="https://www.youtube.com/@AIBTCVIP" target="_blank" rel="noopener noreferrer"
         class="p-1.5 rounded-lg text-slate-400 hover:text-red-400 hover:bg-slate-800/50 transition-all duration-200" title="YouTube" aria-label="YouTube">
        <svg class="w-4 h-4" fill="currentColor" viewBox="0 0 24 24">${this.icons.youtube}</svg>
      </a>
    `;

    html += '<div class="w-px h-5 bg-slate-700 mx-1"></div>';

    // 用户菜单（非公开模式）
    if (!isPublicMode) {
      // 动态生成用户菜单项
      const userMenuItems = this.navItems.user
        .filter(item => !item.requireAuth || isPublicMode === false)
        .map(item => {
          const colorClass = item.color === 'amber' ? 'text-amber-400 hover:text-amber-300' : 'text-slate-300 hover:text-white';
          const iconColor = item.color === 'amber' ? '' : 'text-slate-400';
          let icon = '';
          if (item.id === 'statistics') {
            icon = '<path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 19v-6a2 2 0 00-2-2H5a2 2 0 00-2 2v6a2 2 0 002 2h2a2 2 0 002-2zm0 0V9a2 2 0 012-2h2a2 2 0 012 2v10m-6 0a2 2 0 002 2h2a2 2 0 002-2m0 0V5a2 2 0 012-2h2a2 2 0 012 2v14a2 2 0 01-2 2h-2a2 2 0 01-2-2z"/>';
          } else if (item.id === 'leaderboard') {
            icon = this.icons.chart;
          } else if (item.id === 'referral') {
            icon = '<path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 4.354a4 4 0 110 5.292M15 21H3v-1a6 6 0 0112 0v1zm0 0h6v-1a6 6 0 00-9-5.197M13 7a4 4 0 11-8 0 4 4 0 018 0z"/>';
          } else if (item.id === 'settings') {
            icon = this.icons.settings;
          }
          return `
            <a href="${item.href}" 
               class="flex items-center gap-2 px-3 py-2 text-sm ${colorClass} hover:bg-slate-700/50 transition-colors">
              <svg class="w-4 h-4 ${iconColor}" fill="none" stroke="currentColor" viewBox="0 0 24 24">${icon}</svg>
              ${this.t(item.labelKey)}
            </a>
          `;
        }).join('');

      html += `
        <div class="relative group">
          <button class="px-2 py-1.5 rounded-lg text-sm text-slate-400 hover:text-white hover:bg-slate-800/50 transition-all duration-200 flex items-center gap-1">
            <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">${this.icons.users}</svg>
            ${this.t('nav.myAccount')}
            <svg class="w-3 h-3 transition-transform group-hover:rotate-180" fill="none" stroke="currentColor" viewBox="0 0 24 24">${this.icons.chevronDown}</svg>
          </button>
          <div class="nav-dropdown absolute top-full right-0 mt-1 w-36 py-1.5 bg-slate-800/95 backdrop-blur-xl border border-slate-700/50 rounded-xl shadow-xl transition-all duration-200 z-50">
            ${userMenuItems}
            ${this.config.isAdmin ? `
              <a href="/admin.html" 
                 class="flex items-center gap-2 px-3 py-2 text-sm text-purple-400 hover:text-purple-300 hover:bg-slate-700/50 transition-colors">
                <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">${this.icons.admin}</svg>
                ${this.t('nav.admin')}
              </a>
            ` : ''}
            <div class="h-px bg-slate-700/50 my-1 mx-2"></div>
            <button onclick="SharedNav.logout()" 
               class="flex items-center gap-2 px-3 py-2 text-sm text-red-400 hover:text-red-300 hover:bg-slate-700/50 transition-colors w-full text-left">
              <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">${this.icons.logout}</svg>
              ${this.t('nav.logout')}
            </button>
          </div>
        </div>
      `;
    }

    // 登录/注册（公开模式）
    if (isPublicMode) {
      html += `
        <a href="/login.html" class="px-2.5 py-1.5 rounded-lg text-sm text-slate-400 hover:text-white hover:bg-slate-800/50 transition-all duration-200">
          ${this.t('nav.login')}
        </a>
        <a href="/login.html" class="px-3 py-1.5 rounded-lg text-sm bg-blue-500 hover:bg-blue-600 text-white transition-all duration-200">
          ${this.t('nav.register')}
        </a>
      `;
    }
    
    // 语言切换器（始终显示）
    html += '<div class="w-px h-5 bg-slate-700 mx-1"></div>';
    html += this.getLanguageSwitcherHTML();

    return html;
  },

  // 移动端导航 HTML - 与 app.js 保持一致
  getMobileNavHTML() {
    const { currentPage, isPublicMode, isHomePage } = this.config;
    let html = '<nav class="flex flex-col gap-0.5">';

    // 主导航
    if (isHomePage) {
      // 主页模式：使用 data-nav-page 属性
      html += `
        <div class="px-3 pb-2">
          <div class="text-[10px] uppercase tracking-wider text-slate-500 font-medium mb-1.5 px-2">${this.t('nav.navigation')}</div>
          ${!isPublicMode ? `
          <a href="#logic" data-nav-page="logic"
             class="py-2.5 px-3 rounded-lg transition font-medium flex items-center gap-3 border ${currentPage === 'logic' ? 'bg-blue-500/20 text-blue-400 border-blue-500/30' : 'text-slate-300 border-transparent hover:bg-slate-800/50'}">
            <svg class="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">${this.icons.brain}</svg>
            ${this.t('nav.aiDecision')}
          </a>
          ` : ''}
          <a href="#dashboard" data-nav-page="dashboard"
             class="py-2.5 px-3 rounded-lg transition font-medium flex items-center gap-3 border ${!isPublicMode ? 'mt-1' : ''} ${currentPage === 'dashboard' ? 'bg-blue-500/20 text-blue-400 border-blue-500/30' : 'text-slate-300 border-transparent hover:bg-slate-800/50'}">
            <svg class="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">${this.icons.chart}</svg>
            ${this.t('nav.dashboard')}
          </a>
        </div>
      `;
    } else {
      // 其他页面：使用完整 href 跳转
      html += `
        <div class="px-3 pb-2">
          <div class="text-[10px] uppercase tracking-wider text-slate-500 font-medium mb-1.5 px-2">${this.t('nav.navigation')}</div>
          ${!isPublicMode ? `
          <a href="/#logic"
             class="py-2.5 px-3 rounded-lg transition font-medium flex items-center gap-3 border ${currentPage === 'logic' ? 'bg-blue-500/20 text-blue-400 border-blue-500/30' : 'text-slate-300 border-transparent hover:bg-slate-800/50'}">
            <svg class="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">${this.icons.brain}</svg>
            ${this.t('nav.aiDecision')}
          </a>
          ` : ''}
          <a href="/#dashboard"
             class="py-2.5 px-3 rounded-lg transition font-medium flex items-center gap-3 border ${!isPublicMode ? 'mt-1' : ''} ${currentPage === 'dashboard' ? 'bg-blue-500/20 text-blue-400 border-blue-500/30' : 'text-slate-300 border-transparent hover:bg-slate-800/50'}">
            <svg class="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">${this.icons.chart}</svg>
            ${this.t('nav.dashboard')}
          </a>
        </div>
      `;
    }

    html += '<div class="h-px bg-slate-700/30 mx-3 my-1"></div>';

    // 交易返佣
    html += `
      <div class="px-3 py-2">
        <div class="text-[10px] uppercase tracking-wider text-emerald-500 font-medium mb-1.5 px-2 flex items-center gap-1.5">
          <svg class="w-3.5 h-3.5" fill="none" stroke="currentColor" viewBox="0 0 24 24">${this.icons.dollar}</svg>
          ${this.t('nav.rebate')}
        </div>
        <div class="grid grid-cols-2 gap-2">
          <a href="https://accounts.binance.com/register?ref=1162334440" target="_blank" rel="noopener noreferrer"
             class="flex items-center gap-2 py-2 px-3 rounded-lg bg-slate-800/30 hover:bg-slate-800/60 transition border border-slate-700/30">
            <span class="w-6 h-6 rounded-full bg-yellow-500/20 flex items-center justify-center text-yellow-400 text-xs font-bold">B</span>
            <span class="text-slate-300 text-sm">Binance</span>
          </a>
          <a href="https://www.okx.com/join/88536686" target="_blank" rel="noopener noreferrer"
             class="flex items-center gap-2 py-2 px-3 rounded-lg bg-slate-800/30 hover:bg-slate-800/60 transition border border-slate-700/30">
            <span class="w-6 h-6 rounded-full bg-white/10 flex items-center justify-center text-white text-xs font-bold">O</span>
            <span class="text-slate-300 text-sm">OKX</span>
          </a>
          <a href="https://app.hyperliquid.xyz/join/AIBTC" target="_blank" rel="noopener noreferrer"
             class="flex items-center gap-2 py-2 px-3 rounded-lg bg-slate-800/30 hover:bg-slate-800/60 transition border border-slate-700/30">
            <span class="w-6 h-6 rounded-full bg-green-500/20 flex items-center justify-center text-green-400 text-xs font-bold">H</span>
            <span class="text-slate-300 text-sm">Hyperliquid</span>
          </a>
          <a href="https://partner.hdmune.cn/bg/J13A5U" target="_blank" rel="noopener noreferrer"
             class="flex items-center gap-2 py-2 px-3 rounded-lg bg-slate-800/30 hover:bg-slate-800/60 transition border border-slate-700/30">
            <span class="w-6 h-6 rounded-full bg-cyan-500/20 flex items-center justify-center text-cyan-400 text-xs font-bold">B</span>
            <span class="text-slate-300 text-sm">Bitget</span>
          </a>
        </div>
      </div>
    `;

    html += '<div class="h-px bg-slate-700/30 mx-3 my-1"></div>';

    // 工具与服务
    html += `
      <div class="px-3 py-2">
        <div class="text-[10px] uppercase tracking-wider text-slate-500 font-medium mb-1.5 px-2">${this.t('nav.toolsServices')}</div>
        <a href="https://token.aibtc.vip/latest" target="_blank" rel="noopener noreferrer"
           class="flex items-center gap-3 py-2.5 px-3 rounded-lg text-slate-300 hover:bg-slate-800/50 transition">
          <svg class="w-5 h-5 text-purple-400" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M13 10V3L4 14h7v7l9-11h-7z"/></svg>
          ${this.t('nav.smartPick')}
        </a>
        <a href="https://api.aibtc.vip/register?aff=hjJ7" target="_blank" rel="noopener noreferrer"
           class="flex items-center gap-3 py-2.5 px-3 rounded-lg text-amber-400 hover:bg-amber-500/10 transition mt-1">
          <svg class="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M8 9l3 3-3 3m5 0h3M5 20h14a2 2 0 002-2V6a2 2 0 00-2-2H5a2 2 0 00-2 2v12a2 2 0 002 2z"/></svg>
          ${this.t('nav.aiProxy')}
          <span class="text-[10px] bg-amber-500/20 text-amber-300 px-1.5 py-0.5 rounded ml-auto">${this.t('nav.discount')}</span>
        </a>
      </div>
    `;

    html += '<div class="h-px bg-slate-700/30 mx-3 my-1"></div>';

    // 社交媒体
    html += `
      <div class="px-3 py-2">
        <div class="text-[10px] uppercase tracking-wider text-slate-500 font-medium mb-1.5 px-2">${this.t('nav.followUs')}</div>
        <div class="flex items-center gap-2 px-2">
          <a href="https://t.me/aibtcchina" target="_blank" rel="noopener noreferrer"
             class="flex-1 flex items-center justify-center gap-2 py-2.5 rounded-lg bg-slate-800/30 hover:bg-blue-500/20 text-slate-400 hover:text-blue-400 transition border border-slate-700/30" aria-label="Telegram">
            <svg class="w-5 h-5" fill="currentColor" viewBox="0 0 24 24">${this.icons.telegram}</svg>
            <span class="text-sm">TG</span>
          </a>
          <a href="https://x.com/Aibtcvip" target="_blank" rel="noopener noreferrer"
             class="flex-1 flex items-center justify-center gap-2 py-2.5 rounded-lg bg-slate-800/30 hover:bg-slate-700/50 text-slate-400 hover:text-white transition border border-slate-700/30" aria-label="X/Twitter">
            <svg class="w-5 h-5" fill="currentColor" viewBox="0 0 24 24">${this.icons.twitter}</svg>
            <span class="text-sm">X</span>
          </a>
          <a href="https://www.youtube.com/@AIBTCVIP" target="_blank" rel="noopener noreferrer"
             class="flex-1 flex items-center justify-center gap-2 py-2.5 rounded-lg bg-slate-800/30 hover:bg-red-500/20 text-slate-400 hover:text-red-400 transition border border-slate-700/30" aria-label="YouTube">
            <svg class="w-5 h-5" fill="currentColor" viewBox="0 0 24 24">${this.icons.youtube}</svg>
            <span class="text-sm">YT</span>
          </a>
        </div>
      </div>
    `;

    html += '<div class="h-px bg-slate-700/30 mx-3 my-1"></div>';

    // 用户功能 - 动态生成
    html += `
      <div class="px-3 py-2">
        <div class="text-[10px] uppercase tracking-wider text-slate-500 font-medium mb-1.5 px-2">${this.t('nav.account')}</div>
        <div class="grid grid-cols-2 gap-2">
    `;

    // 动态生成用户菜单项
    this.navItems.user.forEach(item => {
      if (item.requireAuth && isPublicMode) return;
      
      let icon = '';
      if (item.id === 'statistics') {
        icon = '<path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 19v-6a2 2 0 00-2-2H5a2 2 0 00-2 2v6a2 2 0 002 2h2a2 2 0 002-2zm0 0V9a2 2 0 012-2h2a2 2 0 012 2v10m-6 0a2 2 0 002 2h2a2 2 0 002-2m0 0V5a2 2 0 012-2h2a2 2 0 012 2v14a2 2 0 01-2 2h-2a2 2 0 01-2-2z"/>';
      } else if (item.id === 'leaderboard') {
        icon = this.icons.chart;
      } else if (item.id === 'referral') {
        icon = '<path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 4.354a4 4 0 110 5.292M15 21H3v-1a6 6 0 0112 0v1zm0 0h6v-1a6 6 0 00-9-5.197M13 7a4 4 0 11-8 0 4 4 0 018 0z"/>';
      } else if (item.id === 'settings') {
        icon = this.icons.settings;
      }
      
      const colorClass = item.color === 'amber' 
        ? 'text-amber-400 bg-amber-500/10 hover:bg-amber-500/20 border-amber-500/20' 
        : 'text-slate-300 bg-slate-800/30 hover:bg-slate-800/60 border-slate-700/30';
      const iconColor = item.color === 'amber' ? '' : 'text-slate-400';
      
      html += `
          <a href="${item.href}" 
             class="flex items-center gap-2 py-2.5 px-3 rounded-lg ${colorClass} transition border">
            <svg class="w-4 h-4 ${iconColor}" fill="none" stroke="currentColor" viewBox="0 0 24 24">${icon}</svg>
            <span class="text-sm">${this.t(item.labelKey)}</span>
          </a>
      `;
    });

    // 管理后台 - 仅管理员
    if (this.config.isAdmin) {
      html += `
          <a href="/admin.html" 
             class="flex items-center gap-2 py-2.5 px-3 rounded-lg text-purple-400 bg-purple-500/10 hover:bg-purple-500/20 transition border border-purple-500/20">
            <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">${this.icons.admin}</svg>
            <span class="text-sm">${this.t('nav.admin')}</span>
          </a>
      `;
    }

    // 退出 - 非公开模式
    if (!isPublicMode) {
      html += `
          <button onclick="SharedNav.logout()" 
             class="flex items-center gap-2 py-2.5 px-3 rounded-lg text-red-400 bg-red-500/10 hover:bg-red-500/20 transition border border-red-500/20">
            <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">${this.icons.logout}</svg>
            <span class="text-sm">${this.t('nav.logout')}</span>
          </button>
      `;
    }

    // 登录/注册 - 公开模式
    if (isPublicMode) {
      html += `
          <a href="/login.html" 
             class="flex items-center gap-2 py-2.5 px-3 rounded-lg text-slate-300 bg-slate-800/30 hover:bg-slate-800/60 transition border border-slate-700/30">
            <svg class="w-4 h-4 text-slate-400" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M11 16l-4-4m0 0l4-4m-4 4h14m-5 4v1a3 3 0 01-3 3H6a3 3 0 01-3-3V7a3 3 0 013-3h7a3 3 0 013 3v1"/></svg>
            <span class="text-sm">${this.t('nav.login')}</span>
          </a>
          <a href="/login.html" 
             class="flex items-center gap-2 py-2.5 px-3 rounded-lg text-white bg-blue-500 hover:bg-blue-600 transition border border-blue-500">
            <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M18 9v3m0 0v3m0-3h3m-3 0h-3m-2-5a4 4 0 11-8 0 4 4 0 018 0zM3 20a6 6 0 0112 0v1H3v-1z"/></svg>
            <span class="text-sm">${this.t('nav.register')}</span>
          </a>
      `;
    }

    html += `
        </div>
      </div>
    `;
    
    // 移动端语言切换器
    html += this.getMobileLanguageSwitcherHTML();

    html += '</nav>';
    return html;
  },
  
  // 移动端语言切换器 HTML
  getMobileLanguageSwitcherHTML() {
    if (typeof I18n === 'undefined') return '';
    
    const currentLocale = I18n.getCurrentLocale();
    const locales = I18n.supportedLocales;
    
    return `
      <div class="h-px bg-slate-700/30 mx-3 my-1"></div>
      <div class="px-3 py-2">
        <div class="text-[10px] uppercase tracking-wider text-slate-500 font-medium mb-1.5 px-2 flex items-center gap-1.5">
          <svg class="w-3.5 h-3.5" fill="none" stroke="currentColor" viewBox="0 0 24 24">${this.icons.globe}</svg>
          ${this.t('nav.language')}
        </div>
        <div class="grid grid-cols-3 gap-2">
          ${locales.map(locale => `
            <button onclick="SharedNav.setLocale('${locale.code}')"
               class="flex items-center justify-center gap-1.5 py-2 px-2 rounded-lg text-xs transition border
                      ${locale.code === currentLocale.code 
                        ? 'bg-blue-500/20 text-blue-400 border-blue-500/30' 
                        : 'text-slate-300 bg-slate-800/30 hover:bg-slate-800/60 border-slate-700/30'}">
              <span>${locale.flag || ''}</span>
              <span class="truncate">${locale.code.split('-')[0].toUpperCase()}</span>
            </button>
          `).join('')}
        </div>
      </div>
    `;
  },

  // 绑定事件
  bindEvents() {
    // 先清理旧的事件监听器，防止内存泄漏
    this.removeEventListeners();
    
    const toggle = document.getElementById('nav-mobile-toggle');
    if (toggle) {
      const toggleHandler = () => this.toggleMobileMenu();
      toggle.addEventListener('click', toggleHandler);
      this._eventListeners.push({ element: toggle, type: 'click', handler: toggleHandler });
    }
    
    // 主页模式：绑定页面切换事件
    if (this.config.isHomePage && this.config.onPageChange) {
      const navLinks = document.querySelectorAll('[data-nav-page]');
      navLinks.forEach(link => {
        const clickHandler = (e) => {
          e.preventDefault();
          const page = link.getAttribute('data-nav-page');
          if (page && this.config.onPageChange) {
            this.config.onPageChange(page);
            // 关闭移动端菜单
            if (this.config.mobileMenuOpen) {
              this.toggleMobileMenu();
            }
          }
        };
        link.addEventListener('click', clickHandler);
        this._eventListeners.push({ element: link, type: 'click', handler: clickHandler });
      });
    }
  },
  
  // 移除所有事件监听器
  removeEventListeners() {
    this._eventListeners.forEach(({ element, type, handler }) => {
      if (element) {
        element.removeEventListener(type, handler);
      }
    });
    this._eventListeners = [];
  },
  
  // 销毁导航组件（清理所有资源）
  destroy() {
    this.removeEventListeners();
    const container = document.getElementById('shared-nav');
    if (container) {
      container.innerHTML = '';
    }
  },

  // 切换移动端菜单
  toggleMobileMenu() {
    const menu = document.getElementById('nav-mobile-menu');
    const menuIcon = document.getElementById('nav-menu-icon');
    const closeIcon = document.getElementById('nav-close-icon');
    
    if (!menu) return;
    
    const isHidden = menu.classList.contains('hidden');
    menu.classList.toggle('hidden', !isHidden);
    
    if (menuIcon && closeIcon) {
      menuIcon.classList.toggle('hidden', isHidden);
      closeIcon.classList.toggle('hidden', !isHidden);
    }
    
    this.config.mobileMenuOpen = isHidden;
  },

  // 退出登录
  logout() {
    if (typeof Auth !== 'undefined' && Auth.logout) {
      Auth.logout();
    } else {
      // 清除本地存储并跳转
      localStorage.removeItem('uid');
      localStorage.removeItem('username');
      localStorage.removeItem('token');
      sessionStorage.removeItem('isAdmin');
      window.location.href = '/login.html';
    }
  }
};

// 便捷初始化函数（异步）
async function initSharedNav(options = {}) {
  await SharedNav.init(options);
}

// 导出到全局
window.SharedNav = SharedNav;
window.initSharedNav = initSharedNav;
