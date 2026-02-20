const { createApp } = Vue;

// 国际化辅助函数 - 使用全局 I18n.t()，由 i18n.js 提供
// window.t 已在 i18n.js 中定义，此处不再重复声明

createApp({
  data() {
    // 从 URL hash 读取页面状态
    const hash = window.location.hash.slice(1); // 去掉 #
    const validPages = ['dashboard', 'logic'];
    // 公开模式不允许访问 logic 页面
    const isPublicMode = window.PUBLIC_MODE || false;
    const initialPage = (validPages.includes(hash) && !(isPublicMode && hash === 'logic')) ? hash : 'dashboard';

    // 检查是否有登录用户（不再依赖 token，使用 Cookie 认证）
    const currentUser = window.CURRENT_USER || {};
    const userId = currentUser.uid || "aibtcvip";

    return {
      userId: userId,
      isLoggedIn: !!currentUser.uid, // 通过 uid 判断是否登录，实际认证由 Cookie 处理
      isAdmin: false, // 是否是管理员，mounted 时检查
      // authToken 已废弃，保留变量名以兼容，但不再使用
      
      // 公开模式相关
      isPublicMode: isPublicMode,
      publicToken: window.PUBLIC_TOKEN || '',
      publicDisplayName: '',  // 公开仪表盘显示名称
      publicError: null,      // 公开模式错误信息
      
      loading: true,
      activePage: initialPage,
      activeTab: "positions",
      _requestAbort: null,
      _loadingRequestId: 0,
      initialEquity: null,
      statsLimit: -1,
      statsLimitOptions: [-1],
      closedLimit: 10,
      closedOffset: 0,
      closedTotal: 0,
      statistics: {
        totalPnl: 0,
        winRate: 0,
        totalTrades: 0,
        avgDuration: 0,
        longWins: 0,
        shortWins: 0,
        totalUnrealizedPnl: 0,
        activePositions: 0,
        totalFee: 0,
        totalFunding: 0,
        maxDrawdown: 0,
        maxDrawdownPct: 0,
        winCount: 0,
        lossCount: 0,
        breakevenCount: 0,
        grossProfit: 0,
        grossLoss: 0,
        walletBalance: 0,
      },
      positions: [],
      closedTrades: [],
      equityCurve: [],
      _equityChart: null,
      analysisList: [],        // 摘要列表（V2 API）
      analysisDetailCache: {}, // 详情缓存 {id: detail}
      analysisLimit: 8,
      analysisOffset: 0,
      analysisTotal: 0,
      analysisLoading: false,
      analysisDetailLoading: false, // 详情加载状态
      selectedAnalysisIndex: 0,
      selectedAnalysisId: null,     // 当前选中的详情 ID
      account: null,

      // 峰值回撤字段也建议声明（更清晰）
      maxDrawdownPeakPct: null,
      maxDrawdownPeakAmount: null,
      maxDrawdownPeakFrom: null,
      maxDrawdownPeakTo: null,

      // 新增:移动端菜单状态
      mobileMenuOpen: false,
      // 新增：权益曲线聚合周期（只聚合不裁剪）
      equityAgg: "1D", // 默认 1D，你也可以改成 "15m"
      equityCurveRaw: [], // 后端原始全量
      equityCurveView: [], // 聚合后用于画图

      // 多交易所支持
      enabledExchanges: [], // 用户启用的交易所列表 [{exchange, status, display_name, ...}]
      viewMode: 'combined', // 'combined' 或 'by_exchange'
      selectedExchange: null, // 当前选择的交易所（null = 全部）
      exchangeData: {}, // 按交易所分组的数据
      exchangeSummary: {}, // 多交易所汇总数据
      
      // 单交易所模式（从 /exchange.html?exchange=xxx 进入）
      singleExchangeMode: window.SINGLE_EXCHANGE_MODE || null,
      
      // 币种精度信息（用于格式化显示）
      symbolPrecisions: {}, // {"binance:BTCUSDT": {price_precision: 2, qty_precision: 3}, ...}
    };
  },

  mounted() {
    // 初始化共享导航组件
    this.initSharedNav();
    
    // 显示页面内容（移除加载遮罩）
    if (typeof I18n !== 'undefined' && I18n.showContent) {
      I18n.showContent();
    }
    
    // 公开模式：使用公开 API 加载数据
    if (this.isPublicMode) {
      // 公开模式不从 localStorage 恢复交易所筛选状态
      this.loadPublicData(true);
      
      // 公开模式下如果 URL 是 #logic，修正为 #dashboard
      if (window.location.hash === '#logic') {
        window.location.hash = 'dashboard';
      }
      
      // 监听窗口大小变化,重新渲染图表
      window.addEventListener('resize', this.handleResize);
      
      // 监听浏览器前进/后退
      window.addEventListener('hashchange', this.onHashChange);
      
      // 公开模式不启动交易所状态定时器
      return;
    }
    
    // ===== 以下是正常登录模式 =====
    
    // 检查管理员权限（异步，不阻塞页面加载）
    if (typeof Auth !== 'undefined' && Auth.isAdmin) {
      Auth.isAdmin().then(isAdmin => {
        this.isAdmin = isAdmin;
      }).catch(() => {
        this.isAdmin = false;
      });
    }
    
    // 单交易所模式：锁定到指定交易所
    if (this.singleExchangeMode) {
      this.selectedExchange = this.singleExchangeMode;
    } else {
      // 从 localStorage 恢复交易所筛选状态
      const savedExchange = localStorage.getItem('selectedExchange');
      if (savedExchange && savedExchange !== 'null') {
        this.selectedExchange = savedExchange;
      }
    }
    
    // 并行加载所有数据，提升加载速度
    const promises = [
      this.loadData(true),
      this.loadEnabledExchanges()
    ];
    // 根据当前页面加载对应数据
    if (this.activePage === 'logic') {
      promises.push(this.loadAnalysisHistory());
    }
    // 使用 Promise.allSettled 避免单个失败导致全部失败
    Promise.allSettled(promises).then(results => {
      results.forEach((result, index) => {
        if (result.status === 'rejected' && window.SafeLog) {
          SafeLog.error(`Initial load promise ${index} failed:`, result.reason);
        }
      });
    });

    // 监听窗口大小变化,重新渲染图表
    window.addEventListener('resize', this.handleResize);

    // 监听浏览器前进/后退
    window.addEventListener('hashchange', this.onHashChange);
    
    // 定时刷新交易所状态（每30秒）
    this._exchangeStatusInterval = setInterval(() => {
      this.loadEnabledExchanges();
    }, 30000);
    
    // 页面可见性优化：页面不可见时暂停定时器，减少后台资源消耗
    document.addEventListener('visibilitychange', this._handleVisibilityChange = () => {
      if (document.hidden) {
        // 页面不可见，暂停定时器
        if (this._exchangeStatusInterval) {
          clearInterval(this._exchangeStatusInterval);
          this._exchangeStatusInterval = null;
        }
      } else {
        // 页面可见，恢复定时器并立即刷新一次
        this.loadEnabledExchanges();
        if (!this._exchangeStatusInterval) {
          this._exchangeStatusInterval = setInterval(() => {
            this.loadEnabledExchanges();
          }, 30000);
        }
      }
    });
  },

  beforeUnmount() {
    if (this._requestAbort) {
      this._requestAbort.abort();
      this._requestAbort = null;
    }
    if (this._equityChart) {
      try {
        this._equityChart.destroy();
      } catch (e) {}
      this._equityChart = null;
    }
    // 清理交易所状态定时器
    if (this._exchangeStatusInterval) {
      clearInterval(this._exchangeStatusInterval);
      this._exchangeStatusInterval = null;
    }
    // 清理可见性监听器
    if (this._handleVisibilityChange) {
      document.removeEventListener('visibilitychange', this._handleVisibilityChange);
      this._handleVisibilityChange = null;
    }
    window.removeEventListener('resize', this.handleResize);
    window.removeEventListener('hashchange', this.onHashChange);
  },

  computed: {
    closedHasPrev() {
      return this.closedOffset > 0;
    },
    // 从实际数据中获取唯一的交易所列表（用于过滤按钮）
    exchangesInData() {
      const exchangeSet = new Set();
      // 从持仓中收集
      if (Array.isArray(this.positions)) {
        this.positions.forEach(p => {
          if (p.exchange) exchangeSet.add(p.exchange);
        });
      }
      // 从已关闭交易中收集
      if (Array.isArray(this.closedTrades)) {
        this.closedTrades.forEach(t => {
          if (t.exchange) exchangeSet.add(t.exchange);
        });
      }
      return Array.from(exchangeSet).sort();
    },
    // 是否显示交易所过滤器（数据中有多个交易所 或 用户启用了多个交易所）
    showExchangeFilter() {
      if (this.singleExchangeMode) return false;
      return this.exchangesInData.length > 1 || this.enabledExchanges.length > 1;
    },
    // 用于过滤的交易所列表（合并配置的交易所和数据中的交易所）
    filterableExchanges() {
      // 始终显示所有配置的交易所，方便用户切换查看
      // 同时包含数据中可能出现的但未配置的交易所（兼容旧数据）
      const exchangeSet = new Set();
      
      // 添加所有配置的交易所
      this.enabledExchanges.forEach(ex => {
        if (ex && ex.exchange) {
          exchangeSet.add(ex.exchange);
        }
      });
      
      // 添加数据中出现的交易所（可能包含已删除配置但仍有历史数据的交易所）
      this.exchangesInData.forEach(ex => {
        exchangeSet.add(ex);
      });
      
      // 转换为统一格式
      return Array.from(exchangeSet).sort().map(ex => {
        // 优先使用 enabledExchanges 中的完整信息
        const enabledInfo = this.enabledExchanges.find(e => e && e.exchange === ex);
        if (enabledInfo) {
          return enabledInfo;
        }
        // 否则构造基本信息
        return {
          exchange: ex,
          display_name: this.getExchangeDisplayName(ex),
          status: this.getExchangeStatus(ex),
        };
      });
    },
    // 过滤持仓（根据选择的交易所）
    filteredPositions() {
      if (!this.selectedExchange) return this.positions;
      return this.positions.filter(p => p.exchange === this.selectedExchange);
    },
    // 过滤已关闭交易（根据选择的交易所）
    filteredClosedTrades() {
      if (!this.selectedExchange) return this.closedTrades;
      return this.closedTrades.filter(t => t.exchange === this.selectedExchange);
    },
    totalLiveNetPnl() {
      const arr = Array.isArray(this.filteredPositions) ? this.filteredPositions : [];
      return arr.reduce((sum, p) => {
        const v = Number(p?.liveNetPnl ?? p?.unrealizedPnl ?? 0);
        return sum + (Number.isFinite(v) ? v : 0);
      }, 0);
    },
    maxDrawdownPeakPctSafe() {
      if (this.maxDrawdownPeakPct === null || this.maxDrawdownPeakPct === undefined) return null;
      const v = Number(this.maxDrawdownPeakPct);
      return Number.isFinite(v) ? v : null;
    },
    closedHasNext() {
      const total = Number(this.closedTotal);
      if (Number.isFinite(total) && total >= 0) {
        return this.closedOffset + this.closedLimit < total;
      }
      return Array.isArray(this.closedTrades) && this.closedTrades.length === this.closedLimit;
    },
    totalReturnPct() {
      const base = this.initialEquity !== null ? Number(this.initialEquity) : 0;
      const pnl = Number(this.statistics?.totalPnl ?? 0);
      if (!base || base <= 0) return 0;
      return (pnl / base) * 100;
    },
    calmarRatio() {
      const dd = Number(this.maxDrawdownPeakPctSafe ?? 0);
      const r = Number(this.totalReturnPct ?? 0);
      if (!dd || dd <= 0) return null;
      return r / dd;
    },
    profitFactor() {
      const gp = Number(this.statistics?.grossProfit ?? 0);
      const gl = Number(this.statistics?.grossLoss ?? 0);
      if (!gl || gl <= 0) return gp > 0 ? 999 : 0;
      return gp / gl;
    },
    expectancy() {
      const total = Number(this.statistics?.totalTrades ?? 0);
      const pnl = Number(this.statistics?.totalPnl ?? 0);
      if (!total || total <= 0) return 0;
      return pnl / total;
    },
    analysisListView() {
      return Array.isArray(this.analysisList) ? this.analysisList : [];
    },
    decisionRounds() {
      const total = Number(this.analysisTotal);
      if (Number.isFinite(total) && total > 0) return total;
      return Array.isArray(this.analysisList) ? this.analysisList.length : 0;
    },
    maxDrawdownPctRelInitial() {
      const base = this.initialEquity !== null ? Number(this.initialEquity) : null;
      const dd = Number(this.statistics?.maxDrawdown ?? 0);
      if (!base || base <= 0) return 0;
      return (dd / base) * 100;
    },
    // 当前选中的摘要项（用于列表高亮）
    selectedAnalysisSummary() {
      if (!Array.isArray(this.analysisList) || this.analysisList.length === 0) {
        return null;
      }
      const i = Math.min(
        Math.max(0, this.selectedAnalysisIndex),
        this.analysisList.length - 1
      );
      return this.analysisList[i];
    },
    // 当前选中的详情（从缓存获取，用于详情展示）
    selectedAnalysis() {
      // 从缓存获取详情（V2 API 模式，登录和公开模式统一）
      if (this.selectedAnalysisId && this.analysisDetailCache[this.selectedAnalysisId]) {
        return this.analysisDetailCache[this.selectedAnalysisId];
      }
      // 如果没有缓存，返回 null（等待加载）
      return null;
    },
    parsedRequest() {
      const req = this.selectedAnalysis?.request;
      if (!req) return null;
      const raw = req.request;
      if (raw && typeof raw === "object") return raw;
      if (typeof raw === "string") {
        const m = raw.match(/<JSON>([\s\S]*?)<\/JSON>/);
        if (m) {
          try {
            return JSON.parse(m[1]);
          } catch {
            return req;
          }
        }
        try {
          return JSON.parse(raw);
        } catch {
          return req;
        }
      }
      return req;
    },
    // 格式化的上文（system_prompt），用于友好展示
    formattedSystemPrompt() {
      const req = this.parsedRequest;
      if (!req || !req._system_prompt) return null;
      const parts = req._system_prompt;
      if (!Array.isArray(parts)) return null;
      // 将数组用分隔线连接，返回纯文本（\n 会被 pre 标签正确渲染）
      return parts.join('\n\n---\n\n');
    },
    // 排除 _system_prompt 的投喂数据
    requestWithoutPrompt() {
      const req = this.parsedRequest;
      if (!req) return null;
      const { _system_prompt, ...rest } = req;
      return rest;
    },
    normalizedSignals() {
      const resp = this.selectedAnalysis?.response;
      if (!resp) return [];

      let arr = [];
      if (Array.isArray(resp.signals) && resp.signals.length > 0) {
        arr = resp.signals;
      } else {
        const raw = resp.content;
        if (typeof raw === "string" && raw) {
          const m = raw.match(/<decision>([\s\S]*?)<\/decision>/);
          if (m) {
            try {
              const parsed = JSON.parse(m[1]);
              arr = Array.isArray(parsed) ? parsed : [];
            } catch {
              arr = [];
            }
          }
        }
      }

      const ACTION_PRIORITY = [
        "open_long_market", "open_short_market", "open_long_limit", "open_short_limit",
        "open_long", "open_short", "close_long", "close_short", "cancel",
        "stop_orders", "update_stop_loss", "update_take_profit", "increase_position",
        "decrease_position", "reverse", "hold", "wait",
      ];

      const rankMap = Object.fromEntries(ACTION_PRIORITY.map((a, i) => [a, i]));
      const getRank = (action) => {
        const k = String(action || "").toLowerCase();
        return rankMap[k] ?? 999;
      };
      const num = (v) => {
        const x = Number(v);
        return Number.isFinite(x) ? x : -Infinity;
      };

      return arr.slice().sort((a, b) => {
        const ra = getRank(a.action);
        const rb = getRank(b.action);
        if (ra !== rb) return ra - rb;
        const sa = num(a.position_size);
        const sb = num(b.position_size);
        if (sa !== sb) return sb - sa;
        return String(a.symbol || "").localeCompare(String(b.symbol || ""));
      });
    },
    signalsSummary() {
      const arr = this.normalizedSignals;
      const total = arr.length;
      const wait = arr.filter((s) => (s.action || "").toLowerCase() === "wait").length;
      const hold = arr.filter((s) => (s.action || "").toLowerCase() === "hold").length;
      const signal = total - wait - hold; // 真正的交易信号（不包括wait和hold）
      return { total, wait, hold, signal };
    },
  },

  methods: {
    // 国际化翻译函数
    t(key, params) {
      // Safe check for global t() function
      return typeof t === 'function' ? t(key, params) : key;
    },
    
    // 初始化共享导航组件
    initSharedNav() {
      if (typeof SharedNav === 'undefined') {
        if (window.SafeLog) SafeLog.warn('SharedNav not loaded');
        return;
      }
      
      const vm = this;
      SharedNav.init({
        currentPage: this.activePage,
        isPublicMode: this.isPublicMode,
        publicDisplayName: this.publicDisplayName,
        isHomePage: true, // 主页模式：页面内切换
        checkAdmin: !this.isPublicMode, // 公开模式不检查管理员
        onPageChange: (page) => {
          // 页面切换回调
          // 公开模式不允许访问 logic 页面
          if (page === 'logic' && !vm.isPublicMode) {
            vm.gotoLogic();
          } else if (page === 'dashboard') {
            vm.gotoDashboard();
          }
          // 更新导航高亮
          SharedNav.setCurrentPage(page);
        }
      });
    },
    
    // Action 样式类
    getActionClass(action) {
      const a = (action || '').toLowerCase();
      if (a.startsWith('close')) return 'bg-red-500/20 text-red-400 ring-1 ring-red-500/30';
      if (a.startsWith('open')) return 'bg-green-500/20 text-green-400 ring-1 ring-green-500/30';
      if (a === 'stop_orders') return 'bg-cyan-500/20 text-cyan-400 ring-1 ring-cyan-500/30';
      if (a.startsWith('update')) return 'bg-orange-500/20 text-orange-400 ring-1 ring-orange-500/30';
      if (a.startsWith('increase') || a.startsWith('decrease')) return 'bg-blue-500/20 text-blue-400 ring-1 ring-blue-500/30';
      if (a === 'reverse') return 'bg-purple-500/20 text-purple-400 ring-1 ring-purple-500/30';
      if (a === 'hold') return 'bg-slate-500/20 text-slate-300 ring-1 ring-slate-500/30';
      if (a === 'wait') return 'bg-yellow-500/20 text-yellow-300 ring-1 ring-yellow-500/30';
      if (a === 'cancel') return 'bg-pink-500/20 text-pink-400 ring-1 ring-pink-500/30';
      return 'bg-slate-600/20 text-slate-400 ring-1 ring-slate-600/30';
    },
    // Action 格式化显示
    formatAction(action) {
      if (!action) return 'unknown';
      return action;
    },
    gotoDashboard() {
      this.activePage = "dashboard";
      this.mobileMenuOpen = false;
      window.location.hash = "dashboard";
      // 更新导航高亮
      if (typeof SharedNav !== 'undefined') {
        SharedNav.setCurrentPage('dashboard');
      }
      this.$nextTick(() => {
        requestAnimationFrame(() => {
          this.renderOrUpdateEquityChart(this.equityCurveView);
        });
      });
    },
    orderTypeLabel(v) {
      const x = String(v || "").toUpperCase();
      if (x === "MARKET") return this.t('dashboard.marketOrder');
      if (x === "LIMIT") return this.t('dashboard.limitOrder');
      return "—";
    },
    getLivePnl(pos) {
      return Number(pos?.liveNetPnl ?? pos?.unrealizedPnl ?? 0);
    },
    calcPct(value, trade) {
      // 分批加仓/减仓：用 maxAbsQty 才能代表这轮交易最大资金占用
      const qty = Number(trade.maxAbsQty || trade.openQty || 0);
      const price = Number(trade.avgOpenPrice || 0);

      const base = Math.abs(qty) * price; // 名义本金（绝对值）
      if (!base || !Number.isFinite(base)) return null;

      return (Number(value) / base) * 100;
    },
    calcDdFromPeakPct(trade) {
      const dd = Number(trade.drawdownToClose || 0);
      const peak = Number(trade.peakPnl || 0);

      // 只有 peak>0 且 dd>0 才有意义
      if (!Number.isFinite(dd) || !Number.isFinite(peak) || dd <= 0 || peak <= 0) return null;

      return (dd / peak) * 100;
    },
    gotoLogic() {
      // 公开模式不允许访问 AI 决策页面
      if (this.isPublicMode) {
        return;
      }
      this.activePage = "logic";
      this.mobileMenuOpen = false;
      window.location.hash = "logic";
      // 更新导航高亮
      if (typeof SharedNav !== 'undefined') {
        SharedNav.setCurrentPage('logic');
      }
      if (!this.analysisList || this.analysisList.length === 0) {
        this.loadAnalysisHistory();
      }
    },
    onHashChange() {
      const hash = window.location.hash.slice(1);
      // 公开模式不允许访问 logic 页面
      if (hash === 'logic' && this.activePage !== 'logic' && !this.isPublicMode) {
        this.activePage = 'logic';
        if (!this.analysisList || this.analysisList.length === 0) {
          this.loadAnalysisHistory();
        }
      } else if (hash === 'dashboard' && this.activePage !== 'dashboard') {
        this.activePage = 'dashboard';
      } else if (hash === 'logic' && this.isPublicMode) {
        // 公开模式下尝试访问 logic，重定向到 dashboard
        window.location.hash = 'dashboard';
      }
    },
    toggleMobileMenu() {
      this.mobileMenuOpen = !this.mobileMenuOpen;
    },
    handleResize() {
      if (this._equityChart) {
        this._equityChart.resize();
      }
    },
    async loadData(resetClosedPaging = false, showLoading = true) {
      if (!this.isLoggedIn) return;
      const reqId = ++this._loadingRequestId;
      if (this._requestAbort) this._requestAbort.abort();
      this._requestAbort = new AbortController();
      if (resetClosedPaging) this.closedOffset = 0;

      // 只有需要时才显示全屏加载（分页时不显示）
      if (showLoading) {
        this.loading = true;
      }

      try {
        // 调用历史统计 API
        await this._loadHistoryData(reqId, resetClosedPaging);
      } catch (e) {
        if (e?.name !== "AbortError" && window.SafeLog) SafeLog.error(e);
      } finally {
        if (reqId !== this._loadingRequestId) return;
        this.loading = false;
        this.$nextTick(() => {
          requestAnimationFrame(() => {
            this.renderOrUpdateEquityChart(this.equityCurveView);
          });
        });
      }
    },
    
    // 历史统计数据加载
    async _loadHistoryData(reqId, resetClosedPaging) {
      // 构建 URL，如果选择了特定交易所则添加 exchange 参数
      let url =
        `/api/dashboard` +
        `?limit=${this.statsLimit}` +
        `&closed_limit=${this.closedLimit}` +
        `&offset=${this.closedOffset}`;
      
      // 如果选择了特定交易所，添加 exchange 参数
      if (this.selectedExchange) {
        url += `&exchange=${this.selectedExchange}`;
      }

      const resp = await fetch(url, {
        signal: this._requestAbort.signal,
        credentials: 'include' // 使用 HttpOnly Cookie 认证
      });

      // 处理各种 HTTP 错误状态
      if (resp.status === 401) {
        window.location.href = '/login.html';
        return;
      }
      
      if (resp.status === 403) {
        throw new Error(t('error.requestDenied'));
      }
      
      if (resp.status >= 500) {
        throw new Error(t('error.serverError'));
      }
      
      if (!resp.ok) {
        throw new Error(`HTTP ${resp.status}`);
      }

      // 安全的 JSON 解析
      let dash;
      try {
        dash = await resp.json();
      } catch (e) {
        if (window.SafeLog) SafeLog.error('Failed to parse dashboard response:', e);
        throw new Error(t('error.invalidResponse'));
      }

      if (reqId !== this._loadingRequestId) return;

      // 统计兜底合并（别再覆盖回 dash.statistics）
      const s = dash.statistics || {};

      const wallet =
        dash.walletBalance ??
        dash.account?.walletBalance ??
        s.walletBalance ??
        null;

      this.statistics = {
        ...this.statistics,
        ...s,
        walletBalance: wallet ?? this.statistics.walletBalance ?? 0,
      };

      // 其它数据正常赋值
      this.positions = dash.positions || [];
      this.closedTrades = dash.closedTrades || [];
      // 保存原始全量
      this.equityCurveRaw = Array.isArray(dash.equityCurve) ? dash.equityCurve : [];

      // 生成聚合后的视图曲线
      this.equityCurveView = this.aggregateEquityCurve(this.equityCurveRaw, this.equityAgg);

      // （可选）保留旧字段，避免你其它地方引用报错
      this.equityCurve = this.equityCurveRaw;
      this.initialEquity = dash.initialEquity ?? s.initialEquity ?? null;
      this.closedTotal = Number(dash.closedTotal ?? 0) || 0;
      this.account = dash.account || null;
      this.maxDrawdownPeakPct = dash.maxDrawdownPeakPct ?? null;
      this.maxDrawdownPeakAmount = dash.maxDrawdownPeakAmount ?? null;
      this.maxDrawdownPeakFrom = dash.maxDrawdownPeakFrom ?? null;
      this.maxDrawdownPeakTo = dash.maxDrawdownPeakTo ?? null;
      
      // 币种精度信息
      this.symbolPrecisions = dash.symbolPrecisions || {};
    },
    prevClosedPage() {
      if (!this.closedHasPrev) return;
      this.closedOffset = Math.max(0, this.closedOffset - this.closedLimit);
      this.loadData(false, false); // 分页时不显示全屏加载
    },
    nextClosedPage() {
      if (!this.closedHasNext) return;
      this.closedOffset = this.closedOffset + this.closedLimit;
      this.loadData(false, false); // 分页时不显示全屏加载
    },
    
    // ===== AI 决策历史（V2 优化版：摘要列表 + 按需加载详情）=====
    async loadAnalysisHistory() {
      if (!this.isLoggedIn) return;
      this.analysisLoading = true;
      try {
        // 使用 V2 API 获取摘要列表（数据量减少 90%+）
        const url =
          `/api/analysis-history-v2/` +
          `?limit=${this.analysisLimit}&offset=${this.analysisOffset}`;

        const resp = await fetch(url, {
          credentials: 'include'
        });

        if (resp.status === 401) {
          window.location.href = '/login.html';
          return;
        }
        
        if (!resp.ok) {
          throw new Error(`HTTP ${resp.status}`);
        }

        // 安全的 JSON 解析
        let data;
        try {
          data = await resp.json();
        } catch (e) {
          if (window.SafeLog) SafeLog.error('Failed to parse analysis history response:', e);
          throw new Error(t('error.invalidResponse'));
        }

        this.analysisList = Array.isArray(data.items) ? data.items : [];
        this.analysisTotal = Number(data.total || 0) || this.analysisList.length;
        this.selectedAnalysisIndex = 0;
        this.selectedAnalysisId = null;
        
        // 清空详情缓存（分页后旧缓存无效）
        this.analysisDetailCache = {};
        
        // 自动加载第一条的详情
        if (this.analysisList.length > 0 && this.analysisList[0].id) {
          this.selectAnalysisItem(0);
        }
      } catch (e) {
        if (window.SafeLog) SafeLog.error("loadAnalysisHistory failed", e);
      } finally {
        this.analysisLoading = false;
      }
    },
    
    // 选择列表项并加载详情
    async selectAnalysisItem(index) {
      if (index < 0 || index >= this.analysisList.length) return;
      
      this.selectedAnalysisIndex = index;
      const item = this.analysisList[index];
      
      if (!item || !item.id) return;
      
      // 如果缓存中有，直接使用
      if (this.analysisDetailCache[item.id]) {
        this.selectedAnalysisId = item.id;
        return;
      }
      
      // 否则加载详情
      await this.loadAnalysisDetail(item.id);
    },
    
    // 加载单条详情
    async loadAnalysisDetail(id) {
      if (!id) return;
      
      // 已缓存则跳过
      if (this.analysisDetailCache[id]) {
        this.selectedAnalysisId = id;
        return;
      }
      
      this.analysisDetailLoading = true;
      try {
        const resp = await fetch(`/api/analysis-history-v2/${id}`, {
          credentials: 'include'
        });
        
        if (resp.status === 401) {
          window.location.href = '/login.html';
          return;
        }
        
        if (!resp.ok) {
          if (window.SafeLog) SafeLog.error(`Failed to load analysis detail: ${resp.status}`);
          return;
        }
        
        const detail = await resp.json();
        
        // 缓存详情
        this.analysisDetailCache[id] = detail;
        this.selectedAnalysisId = id;
      } catch (e) {
        if (window.SafeLog) SafeLog.error("loadAnalysisDetail failed", e);
      } finally {
        this.analysisDetailLoading = false;
      }
    },
    
    // 公开模式：加载 AI 决策历史摘要（V2 API）
    async loadPublicAnalysisHistory() {
      if (!this.isPublicMode || !this.publicToken) return;
      this.analysisLoading = true;
      try {
        // 使用 V2 API 获取摘要列表
        const url =
          `/api/public/analysis-history-v2/${this.publicToken}` +
          `?limit=${this.analysisLimit}&offset=${this.analysisOffset}`;

        const resp = await fetch(url);

        if (!resp.ok) {
          if (resp.status === 404) {
            this.publicError = t('error.notFound');
          } else if (resp.status === 429) {
            this.publicError = t('error.tooManyRequests');
          }
          return;
        }

        const data = await resp.json();

        this.analysisList = Array.isArray(data.items) ? data.items : [];
        this.analysisTotal = Number(data.total || 0) || this.analysisList.length;
        this.selectedAnalysisIndex = 0;
        this.selectedAnalysisId = null;
        
        // 清空详情缓存（分页后旧缓存无效）
        this.analysisDetailCache = {};
        
        // 自动加载第一条的详情
        if (this.analysisList.length > 0 && this.analysisList[0].id) {
          this.selectPublicAnalysisItem(0);
        }
      } catch (e) {
        if (window.SafeLog) SafeLog.error("loadPublicAnalysisHistory failed", e);
      } finally {
        this.analysisLoading = false;
      }
    },
    
    // 公开模式：选择列表项并加载详情
    async selectPublicAnalysisItem(index) {
      if (index < 0 || index >= this.analysisList.length) return;
      
      this.selectedAnalysisIndex = index;
      const item = this.analysisList[index];
      
      if (!item || !item.id) return;
      
      // 如果缓存中有，直接使用
      if (this.analysisDetailCache[item.id]) {
        this.selectedAnalysisId = item.id;
        return;
      }
      
      // 否则加载详情
      await this.loadPublicAnalysisDetail(item.id);
    },
    
    // 公开模式：加载单条详情
    async loadPublicAnalysisDetail(id) {
      if (!id || !this.publicToken) return;
      
      // 已缓存则跳过
      if (this.analysisDetailCache[id]) {
        this.selectedAnalysisId = id;
        return;
      }
      
      this.analysisDetailLoading = true;
      try {
        const resp = await fetch(`/api/public/analysis-history-v2/${this.publicToken}/${id}`);
        
        if (!resp.ok) {
          if (resp.status === 404) {
            if (window.SafeLog) SafeLog.warn('Analysis record not found');
          } else if (resp.status === 429) {
            if (window.SafeLog) SafeLog.warn('Too many requests');
          }
          return;
        }
        
        const detail = await resp.json();
        
        // 缓存详情
        this.analysisDetailCache[id] = detail;
        this.selectedAnalysisId = id;
      } catch (e) {
        if (window.SafeLog) SafeLog.error("loadPublicAnalysisDetail failed", e);
      } finally {
        this.analysisDetailLoading = false;
      }
    },
    
    // ===== 公开模式数据加载 =====
    async loadPublicData(showLoading = true, resetClosedPaging = true) {
      if (!this.isPublicMode || !this.publicToken) {
        this.publicError = t('error.invalidLink');
        this.loading = false;
        return;
      }
      
      if (showLoading) {
        this.loading = true;
      }
      this.publicError = null;
      
      if (resetClosedPaging) {
        this.closedOffset = 0;
      }
      
      // 取消之前的请求
      if (this._requestAbort) {
        this._requestAbort.abort();
      }
      this._requestAbort = new AbortController();
      const reqId = ++this._loadingRequestId;
      
      try {
        // 构建 URL，支持分页和交易所筛选
        let url = `/api/public/dashboard/${this.publicToken}` +
          `?limit=${this.statsLimit}` +
          `&closed_limit=${this.closedLimit}` +
          `&offset=${this.closedOffset}`;
        
        // 如果选择了特定交易所，添加 exchange 参数
        if (this.selectedExchange) {
          url += `&exchange=${this.selectedExchange}`;
        }
        
        const resp = await fetch(url, {
          signal: this._requestAbort.signal
        });
        
        if (!resp.ok) {
          if (resp.status === 404) {
            this.publicError = t('error.notFound');
          } else if (resp.status === 429) {
            this.publicError = t('error.tooManyRequests');
          } else {
            this.publicError = `${t('error.loadFailed')} (${resp.status})`;
          }
          this.loading = false;
          return;
        }
        
        const data = await resp.json();
        
        if (reqId !== this._loadingRequestId) return;
        
        // 保存公开显示名称
        this.publicDisplayName = data.display_name || '';
        
        // 更新导航显示名称（公开模式）
        if (typeof SharedNav !== 'undefined' && this.publicDisplayName) {
          SharedNav.config.publicDisplayName = this.publicDisplayName;
          SharedNav.render();
          SharedNav.bindEvents();
        }
        
        // 映射统计数据
        const s = data.statistics || {};
        this.statistics = {
          ...this.statistics,
          ...s,
          walletBalance: data.walletBalance ?? s.walletBalance ?? this.statistics.walletBalance ?? 0,
        };
        
        // 映射其他数据
        this.positions = data.positions || [];
        this.closedTrades = data.closedTrades || [];
        this.equityCurveRaw = Array.isArray(data.equityCurve) ? data.equityCurve : [];
        this.equityCurveView = this.aggregateEquityCurve(this.equityCurveRaw, this.equityAgg);
        this.equityCurve = this.equityCurveRaw;
        this.initialEquity = data.initialEquity ?? s.initialEquity ?? null;
        this.closedTotal = Number(data.closedTotal ?? 0) || 0;
        this.account = data.account || null;
        this.maxDrawdownPeakPct = data.maxDrawdownPeakPct ?? null;
        this.maxDrawdownPeakAmount = data.maxDrawdownPeakAmount ?? null;
        this.maxDrawdownPeakFrom = data.maxDrawdownPeakFrom ?? null;
        this.maxDrawdownPeakTo = data.maxDrawdownPeakTo ?? null;
        
        // 交易所列表
        this.enabledExchanges = data.enabledExchanges || [];
        
        // 币种精度信息
        this.symbolPrecisions = data.symbolPrecisions || {};
        
      } catch (e) {
        if (e.name === 'AbortError') return;
        if (window.SafeLog) SafeLog.error("loadPublicData failed", e);
        this.publicError = t('error.networkError');
      } finally {
        if (reqId === this._loadingRequestId) {
          this.loading = false;
          // 渲染权益曲线图表
          this.$nextTick(() => {
            requestAnimationFrame(() => {
              this.renderOrUpdateEquityChart(this.equityCurveView);
            });
          });
        }
      }
    },
    
    // 公开模式：切换交易所筛选
    selectPublicExchange(exchange) {
      const newExchange = (exchange === null) ? null : (exchange === this.selectedExchange ? null : exchange);
      if (newExchange !== this.selectedExchange) {
        this.selectedExchange = newExchange;
        this.closedOffset = 0;
        this.loadPublicData(true, false);
      }
    },
    
    // 公开模式：分页
    publicPrevPage() {
      if (this.closedOffset <= 0) return;
      this.closedOffset = Math.max(0, this.closedOffset - this.closedLimit);
      this.loadPublicData(false, false);
    },
    publicNextPage() {
      if (this.closedOffset + this.closedLimit >= this.closedTotal) return;
      this.closedOffset = this.closedOffset + this.closedLimit;
      this.loadPublicData(false, false);
    },
    
    // 加载启用的交易所列表
    async loadEnabledExchanges() {
      if (!this.isLoggedIn) return;
      try {
        const resp = await fetch('/api/user/exchanges/enabled', {
          credentials: 'include' // 使用 HttpOnly Cookie 认证
        });
        if (resp.ok) {
          const data = await resp.json();
          // enabledExchanges 存储完整对象数组 [{exchange: 'binance', status: 'connected', ...}]
          this.enabledExchanges = data.enabled_exchanges || [];
          
          // 验证 selectedExchange 是否有效（防止 localStorage 中保存了已删除的交易所）
          if (this.selectedExchange) {
            const enabledExchangeNames = this.enabledExchanges.map(ex => ex.exchange);
            if (!enabledExchangeNames.includes(this.selectedExchange)) {
              // selectedExchange 不在启用列表中，重置为全局并重新加载
              if (window.SafeLog) SafeLog.warn(`[loadEnabledExchanges] selectedExchange "${this.selectedExchange}" not in enabled list, resetting to global`);
              this.selectedExchange = null;
              localStorage.setItem('selectedExchange', 'null');
              // 重新加载数据（使用全局模式）
              this.loadData(true, false);
            }
          }
          
          // 如果有多个交易所，自动加载交易所数据
          if (this.enabledExchanges.length > 1) {
            this.loadExchangeData();
          }
        }
      } catch (e) {
        if (window.SafeLog) SafeLog.error("loadEnabledExchanges failed", e);
      }
    },
    
    // 切换视图模式（合并/按交易所）
    toggleViewMode() {
      this.viewMode = this.viewMode === 'combined' ? 'by_exchange' : 'combined';
      if (this.viewMode === 'by_exchange') {
        this.loadExchangeData();
      }
    },
    
    // 选择交易所筛选
    selectExchange(exchange) {
      // 如果点击的是当前选中的交易所，则切回全局；如果点击的是 null（全局），则设为 null
      const newExchange = (exchange === null) ? null : (exchange === this.selectedExchange ? null : exchange);
      if (newExchange !== this.selectedExchange) {
        this.selectedExchange = newExchange;
        // 保存到 localStorage（非单交易所模式）
        if (!this.singleExchangeMode) {
          localStorage.setItem('selectedExchange', newExchange === null ? 'null' : newExchange);
        }
        // 切换交易所时重新加载数据（重置分页，但不显示全屏 loading 以避免页面跳动）
        this.closedOffset = 0;
        this.loadData(true, false);
      }
    },
    
    // 加载按交易所分组的数据
    async loadExchangeData() {
      if (!this.isLoggedIn) return;
      try {
        const resp = await fetch('/api/dashboard/exchanges', {
          credentials: 'include' // 使用 HttpOnly Cookie 认证
        });
        if (resp.ok) {
          const data = await resp.json();
          this.exchangeData = data.exchanges || {};
          this.exchangeSummary = data.summary || {};
        }
      } catch (e) {
        if (window.SafeLog) SafeLog.error("loadExchangeData failed", e);
      }
    },
    
    // 获取交易所颜色类
    getExchangeColorClass(exchange) {
      const colors = {
        'binance': 'bg-yellow-500/20 text-yellow-400 border-yellow-500/30',
        'okx': 'bg-blue-500/20 text-blue-400 border-blue-500/30',
        'bitget': 'bg-green-500/20 text-green-400 border-green-500/30',
        'hyperliquid': 'bg-purple-500/20 text-purple-400 border-purple-500/30',
      };
      return colors[exchange] || 'bg-slate-500/20 text-slate-400 border-slate-500/30';
    },
    
    // 获取交易所背景色（用于卡片）
    getExchangeBgClass(exchange) {
      const colors = {
        'binance': 'bg-gradient-to-br from-yellow-500/10 to-slate-800/50 border-yellow-500/20',
        'okx': 'bg-gradient-to-br from-blue-500/10 to-slate-800/50 border-blue-500/20',
        'bitget': 'bg-gradient-to-br from-green-500/10 to-slate-800/50 border-green-500/20',
        'hyperliquid': 'bg-gradient-to-br from-purple-500/10 to-slate-800/50 border-purple-500/20',
      };
      return colors[exchange] || 'bg-slate-800/50 border-slate-700/30';
    },
    
    // 获取交易所文字色
    getExchangeTextClass(exchange) {
      const colors = {
        'binance': 'text-yellow-400',
        'okx': 'text-blue-400',
        'bitget': 'text-green-400',
        'hyperliquid': 'text-purple-400',
      };
      return colors[exchange] || 'text-slate-400';
    },
    
    // 获取连接状态指示器颜色
    getConnectionStatusClass(status) {
      const statusColors = {
        'connected': 'bg-green-500',
        'connecting': 'bg-yellow-500',
        'stale': 'bg-yellow-500',
        'disconnected': 'bg-red-500',
        'error': 'bg-red-500',
        'auth_failed': 'bg-orange-500',
        'unknown': 'bg-slate-500',
      };
      return statusColors[status] || 'bg-slate-500';
    },
    
    // 获取连接状态描述
    getConnectionStatusText(status) {
      const statusKeys = {
        'connected': 'exchange.connected',
        'connecting': 'exchange.connecting',
        'stale': 'exchange.stale',
        'disconnected': 'exchange.disconnected',
        'error': 'exchange.error',
        'auth_failed': 'exchange.authFailed',
        'unknown': 'exchange.unknown',
      };
      return this.t(statusKeys[status] || 'exchange.unknown');
    },
    
    // 获取交易所余额
    getExchangeBalance(exchange) {
      const data = this.exchangeData[exchange];
      if (!data || !data.account) return '—';
      const balance = Number(data.account.walletBalance || data.account.equity || 0);
      return balance.toFixed(2);
    },
    
    // 获取交易所未实现盈亏
    getExchangeUnrealizedPnl(exchange) {
      const data = this.exchangeData[exchange];
      if (!data || !data.account) return 0;
      return Number(data.account.unrealized || 0);
    },
    
    // 获取交易所持仓数量
    getExchangePositionCount(exchange) {
      const data = this.exchangeData[exchange];
      if (!data || !data.positions_active) return 0;
      return data.positions_active.length;
    },
    
    // 获取交易所连接状态
    getExchangeStatus(exchange) {
      // 从 enabledExchanges 数组中获取状态
      const exchangeInfo = this.enabledExchanges?.find(ex => ex.exchange === exchange);
      return exchangeInfo?.status || 'unknown';
    },
    
    getExchangeDisplayName(exchange) {
      const names = {
        'binance': 'Binance',
        'okx': 'OKX',
        'bitget': 'Bitget',
        'hyperliquid': 'Hyperliquid'
      };
      return names[exchange] || exchange;
    },
    
    // 获取币种精度信息
    getSymbolPrecision(symbol, exchange) {
      const key = `${exchange || 'binance'}:${symbol}`;
      return this.symbolPrecisions[key] || { price_precision: 2, qty_precision: 3 };
    },
    
    // 格式化价格（使用精度信息）
    formatPriceWithPrecision(value, symbol, exchange) {
      const num = Number(value);
      if (isNaN(num)) return value;
      
      const precision = this.getSymbolPrecision(symbol, exchange);
      return num.toFixed(precision.price_precision);
    },
    
    // 格式化数量（使用精度信息）
    formatQtyWithPrecision(value, symbol, exchange) {
      const num = Number(value);
      if (isNaN(num)) return value;
      
      const precision = this.getSymbolPrecision(symbol, exchange);
      return num.toFixed(precision.qty_precision);
    },
    
    // 原有的 formatPrice（兼容旧代码，根据数值大小自动判断）
    formatPrice(value) {
      const num = Number(value);
      if (isNaN(num)) return value;

      // 使用绝对值判断小数位数
      const absNum = Math.abs(num);

      if (absNum >= 50) {
        return num.toFixed(2);
      } else if (absNum >= 1) {
        return num.toFixed(4);
      } else if (absNum >= 0.01) {
        return num.toFixed(6);
      } else if (absNum >= 0.0001) {
        return num.toFixed(8);
      } else {
        return num.toFixed(10).replace(/\.?0+$/, '');
      }
    },
    // 获取当前 locale（用于日期格式化）
    getLocale() {
      if (typeof I18n !== 'undefined' && I18n.locale) {
        return I18n.locale;
      }
      return 'zh-CN';
    },
    formatTime(ms) {
      const date = new Date(parseInt(ms));
      return date.toLocaleString(this.getLocale(), {
        month: "2-digit",
        day: "2-digit",
        hour: "2-digit",
        minute: "2-digit",
      });
    },
    formatDuration(ms) {
      // 使用紧凑格式：1m, 25m, 1h, 4h, 1d - 各语言通用不用翻译
      const minutes = Math.floor(parseInt(ms) / 60000);
      if (minutes < 60) return `${minutes}m`;
      const hours = Math.floor(minutes / 60);
      if (hours < 24) {
        const remainMins = minutes % 60;
        return remainMins > 0 ? `${hours}h${remainMins}m` : `${hours}h`;
      }
      const days = Math.floor(hours / 24);
      const remainHours = hours % 24;
      return remainHours > 0 ? `${days}d${remainHours}h` : `${days}d`;
    },
    formatUnixSecondsToLocal(tsSeconds) {
      const s = Number(tsSeconds);
      if (!s || !isFinite(s)) return "";
      return new Date(s * 1000).toLocaleString(this.getLocale(), {
        year: "numeric",
        month: "2-digit",
        day: "2-digit",
        hour: "2-digit",
        minute: "2-digit",
        second: "2-digit",
      });
    },

    // 切换权益曲线聚合周期
    setEquityAgg(tf) {
      this.equityAgg = tf;

      // 使用 toRaw 获取原始数据，避免响应式循环
      const rawData = Vue.toRaw ? Vue.toRaw(this.equityCurveRaw) : JSON.parse(JSON.stringify(this.equityCurveRaw));
      const aggregated = this.aggregateEquityCurve(rawData, tf);

      // 直接赋值聚合后的数据
      this.equityCurveView = aggregated;

      // 传入纯数据副本，避免响应式代理
      const curveData = JSON.parse(JSON.stringify(aggregated));
      this.renderOrUpdateEquityChart(curveData);
    },

    // 把 time 统一转成毫秒（兼容 time 是 ms / ISO string）
    _toMs(t) {
      if (t == null) return NaN;
      if (typeof t === "number") return t;
      const ms = Date.parse(t);
      return Number.isFinite(ms) ? ms : NaN;
    },

    // 生成桶 key + 桶起始毫秒（用于排序）
    _bucketOf(ms, tf) {
      const d = new Date(ms);
      const y = d.getFullYear();
      const m = d.getMonth() + 1; // 1-12
      const dd = d.getDate();

      const pad2 = (n) => String(n).padStart(2, "0");

      if (tf === "15m") {
        const step = 15 * 60 * 1000;
        const start = Math.floor(ms / step) * step;
        return { key: String(start), startMs: start };
      }

      if (tf === "1D") {
        const key = `${y}-${pad2(m)}-${pad2(dd)}`;
        // 用本地自然日的 00:00 作为桶起点（排序稳定）
        const startMs = new Date(y, m - 1, dd, 0, 0, 0, 0).getTime();
        return { key, startMs };
      }

      if (tf === "1M") {
        const key = `${y}-${pad2(m)}`;
        const startMs = new Date(y, m - 1, 1, 0, 0, 0, 0).getTime();
        return { key, startMs };
      }

      if (tf === "3M") {
        const q = Math.floor((m - 1) / 3) + 1; // 1-4
        const key = `${y}-Q${q}`;
        const startMonth = (q - 1) * 3; // 0,3,6,9
        const startMs = new Date(y, startMonth, 1, 0, 0, 0, 0).getTime();
        return { key, startMs };
      }

      // 1Y
      const key = `${y}`;
      const startMs = new Date(y, 0, 1, 0, 0, 0, 0).getTime();
      return { key, startMs };
    },

    // 聚合权益曲线：只聚合，不裁剪
    aggregateEquityCurve(raw, tf) {
      const arr = Array.isArray(raw) ? raw : [];
      if (arr.length === 0) return [];

      // 先按时间升序（避免后端无序）
      const sorted = arr
        .map((p) => ({ ...p, __ms: this._toMs(p.time) }))
        .filter((p) => Number.isFinite(p.__ms))
        .sort((a, b) => a.__ms - b.__ms);

      if (sorted.length === 0) return [];

      // 不聚合的兜底（如果你未来加了 "RAW" 选项）
      // if (tf === "RAW") return sorted.map(({__ms, ...p}) => p);

      const map = new Map();
      for (const p of sorted) {
        const { key, startMs } = this._bucketOf(p.__ms, tf);

        const prev = map.get(key);
        if (!prev) {
          map.set(key, {
            key,
            startMs,
            last: p,
            pnlSum: Number(p.pnl ?? 0) || 0,
          });
          continue;
        }

        // 桶内累计 pnl（如果你的 pnl 是“单笔”字段）
        prev.pnlSum += Number(p.pnl ?? 0) || 0;

        // 桶内最后一个点（权益曲线用最后点最合理）
        if (p.__ms >= prev.last.__ms) prev.last = p;
      }

      // 输出按桶起点排序
      const buckets = Array.from(map.values()).sort((a, b) => a.startMs - b.startMs);

      // 输出点：time 取桶内最后点 time；equity 取桶内最后点 equity；pnl 取桶内 pnlSum
      return buckets.map((b) => {
        const last = b.last;
        return {
          time: last.time,
          equity: last.equity,
          pnl: b.pnlSum, // 注意：这里变成“桶内净收益”
        };
      });
    },

    renderOrUpdateEquityChart(curve) {
      if (this._equityRendering) return;
      this._equityRendering = true;

      try {
        const canvas = document.getElementById("equityChart");
        if (!canvas) return;

        const baseEquity = this.initialEquity !== null ? Number(this.initialEquity) : null;

        // 销毁现有图表（动画已禁用，直接销毁即可）
        if (this._equityChart) {
          try {
            this._equityChart.destroy();
          } catch (e) {}
          this._equityChart = null;
        }

        if (!curve || curve.length === 0) return;

        // 根据聚合周期选择合适的时间格式
        const formatTime = (time) => {
          const d = new Date(time);
          const agg = this.equityAgg;
          const locale = typeof I18n !== 'undefined' && I18n.locale ? I18n.locale : 'zh-CN';

          if (agg === '15m') {
            return d.toLocaleString(locale, { month: "2-digit", day: "2-digit", hour: "2-digit", minute: "2-digit" });
          } else if (agg === '1D') {
            return d.toLocaleString(locale, { month: "2-digit", day: "2-digit" });
          } else if (agg === '1M') {
            return d.toLocaleString(locale, { year: "numeric", month: "long" });
          } else if (agg === '3M') {
            const q = Math.floor(d.getMonth() / 3) + 1;
            return `${d.getFullYear()} Q${q}`;
          } else {
            return d.getFullYear().toString();
          }
        };

        // 在曲线数据前面插入初始资金点（如果第一个点不是初始资金）
        let curveWithStart = curve.slice();
        if (baseEquity !== null && curve.length > 0) {
          const firstEquity = Number(curve[0].equity);
          // 检查第一个点是否已经是初始资金附近
          if (Math.abs(firstEquity - baseEquity) > 0.01) {
            // 在第一个数据点之前添加一个初始资金点
            const firstTime = curve[0].time;
            const startTime = new Date(firstTime).getTime() - 1000; // 比第一个点早1秒
            curveWithStart = [
              { time: startTime, equity: baseEquity, pnl: 0 },
              ...curve
            ];
          }
        }

        const labels = curveWithStart.map((p) => formatTime(p.time));

        const rawEquity = curveWithStart.map((p) => Number(p.equity));

        let isAbsoluteEquity = false;
        if (baseEquity !== null && rawEquity.length > 0) {
          const diff = Math.abs(rawEquity[0] - baseEquity);
          isAbsoluteEquity = diff <= Math.max(1, baseEquity * 0.2);
        }

        const pnlCumData =
          isAbsoluteEquity && baseEquity !== null
            ? rawEquity.map((v) => v - baseEquity)
            : rawEquity.slice();

        const equityAbsData = isAbsoluteEquity
          ? rawEquity.slice()
          : baseEquity !== null
          ? pnlCumData.map((v) => v + baseEquity)
          : pnlCumData.slice();

        let peak = -Infinity;
        let peakIdx = 0;
        let troughIdx = 0;
        let maxDD = 0;

        for (let i = 0; i < pnlCumData.length; i++) {
          const e = pnlCumData[i];
          if (e > peak) {
            peak = e;
            peakIdx = i;
          }
          const dd = peak - e;
          if (dd > maxDD) {
            maxDD = dd;
            troughIdx = i;
          }
        }

        const ddStart = Math.min(peakIdx, troughIdx);
        const ddEnd = Math.max(peakIdx, troughIdx);
        const hasDrawdown = maxDD > 0 && ddStart !== ddEnd;

        const drawdownPlugin = {
          id: "drawdownShade",
          afterDatasetsDraw: (chart) => {
            if (!chart.chartArea) return;
            if (!chart.$hasDrawdown) return; // 没有回撤时不绘制

            const s = Number(chart.$ddStart ?? 0);
            const e = Number(chart.$ddEnd ?? 0);
            if (!Number.isFinite(s) || !Number.isFinite(e) || s === e) return;

            const xScale = chart.scales?.x;
            const ticksLen = xScale?.ticks?.length ?? 0;

            if (!xScale || ticksLen <= 0) return;
            if (s < 0 || e < 0 || s >= ticksLen || e >= ticksLen) return;

            const { ctx, chartArea } = chart;
            const x1 = xScale.getPixelForTick(s);
            const x2 = xScale.getPixelForTick(e);

            const left = Math.min(x1, x2);
            const width = Math.abs(x2 - x1);

            ctx.save();
            ctx.fillStyle = "rgba(239,68,68,0.10)";
            ctx.fillRect(left, chartArea.top, width, chartArea.bottom - chartArea.top);

            ctx.strokeStyle = "rgba(239,68,68,0.55)";
            ctx.lineWidth = 1;

            ctx.beginPath();
            ctx.moveTo(x1, chartArea.top);
            ctx.lineTo(x1, chartArea.bottom);
            ctx.stroke();

            ctx.beginPath();
            ctx.moveTo(x2, chartArea.top);
            ctx.lineTo(x2, chartArea.bottom);
            ctx.stroke();

            ctx.restore();
          },
        };

        const tooltipCallbacks = {
          title: (items) => {
            const i = items?.[0]?.dataIndex ?? 0;
            return labels[i] || "";
          },
          label: (ctx) => {
            if (ctx.datasetIndex !== 0) return null;

            const i = ctx.dataIndex;
            const pnl = Number(curveWithStart[i]?.pnl ?? 0);
            const pnlCum = Number(pnlCumData[i] ?? 0);

            const pnlStr = `${pnl >= 0 ? "+" : ""}${pnl.toFixed(2)} USDT`;
            const pnlCumStr = `${pnlCum >= 0 ? "+" : ""}${pnlCum.toFixed(2)} USDT`;

            if (baseEquity !== null) {
              const eqNow = baseEquity + pnlCum;
              const diff = eqNow - baseEquity;
              const diffPct = baseEquity > 0 ? (diff / baseEquity) * 100 : 0;

              return [
                `${t('dashboard.singlePnl')}: ${pnlStr}`,
                `${t('dashboard.cumPnl')}: ${pnlCumStr}`,
                `${t('dashboard.currentEquity')}: ${eqNow.toFixed(2)} USDT`,
                `${t('dashboard.initialCapital')}: ${baseEquity.toFixed(2)} USDT`,
                `${t('dashboard.vsBaseline')}: ${diff >= 0 ? "+" : ""}${diff.toFixed(2)} USDT (${diffPct >= 0 ? "+" : ""}${diffPct.toFixed(2)}%)`,
              ];
            }

            return [`${t('dashboard.singlePnl')}: ${pnlStr}`, `${t('dashboard.cumPnl')}: ${pnlCumStr}`];
          },
        };

        // 根据数据点数量动态调整点的大小
        const pointCount = equityAbsData.length;
        const pointRadius = pointCount > 50 ? 0 : pointCount > 20 ? 2 : 3;
        const pointHoverRadius = pointCount > 50 ? 4 : 6;

        // 总是创建新图表（已在上面销毁旧的）
        const datasets = [
          {
            label: baseEquity !== null ? t('dashboard.equityCurve') : t('dashboard.cumPnl'),
            data: equityAbsData,
            pointRadius: pointRadius,
            pointHoverRadius: pointHoverRadius,
            borderColor: "#3b82f6",
            backgroundColor: (context) => {
              const ctx = context.chart.ctx;
              const gradient = ctx.createLinearGradient(0, 0, 0, context.chart.height);
              gradient.addColorStop(0, "rgba(59, 130, 246, 0.25)");
              gradient.addColorStop(1, "rgba(59, 130, 246, 0.02)");
              return gradient;
            },
            pointBackgroundColor: "#3b82f6",
            pointBorderColor: "#1e40af",
            pointBorderWidth: 1,
            fill: true,
            tension: 0.3,
            borderWidth: 2.5,
          },
        ];

        if (baseEquity !== null) {
          datasets.push({
            label: t('dashboard.initialCapital'),
            data: labels.map(() => baseEquity),
            borderColor: "rgba(148,163,184,0.5)",
            borderWidth: 1,
            borderDash: [8, 4],
            pointRadius: 0,
            fill: false,
            tension: 0,
          });
        }

        // 计算 Y 轴范围，留出一定边距
        const dataMin = Math.min(...equityAbsData);
        const dataMax = Math.max(...equityAbsData);
        const range = dataMax - dataMin || 1;
        const padding = range * 0.08;
        const yMin = Math.min(dataMin - padding, baseEquity !== null ? baseEquity - padding : dataMin - padding);
        const yMax = Math.max(dataMax + padding, baseEquity !== null ? baseEquity + padding : dataMax + padding);

        // 计算百分比范围（相对于初始资金）
        let yPctMin = 0, yPctMax = 0;
        if (baseEquity !== null && baseEquity > 0) {
          yPctMin = ((yMin - baseEquity) / baseEquity) * 100;
          yPctMax = ((yMax - baseEquity) / baseEquity) * 100;
        }

        this._equityChart = new Chart(canvas, {
          type: "line",
          data: { labels, datasets },
          options: {
            responsive: true,
            maintainAspectRatio: false,
            animation: false,
            interaction: { mode: "index", intersect: false },
            layout: {
              padding: { top: 10, right: 10, bottom: 5, left: 5 }
            },
            plugins: {
              legend: { display: false },
              decimation: { enabled: true, algorithm: "lttb", samples: 200 },
              tooltip: {
                enabled: true,
                filter: (item) => item.datasetIndex === 0,
                callbacks: tooltipCallbacks,
                mode: "index",
                intersect: false,
                backgroundColor: "rgba(15, 23, 42, 0.95)",
                titleColor: "#f1f5f9",
                bodyColor: "#cbd5e1",
                borderColor: "rgba(71, 85, 105, 0.5)",
                borderWidth: 1,
                padding: 12,
                cornerRadius: 8,
                displayColors: false,
                titleFont: { size: 13, weight: "600" },
                bodyFont: { size: 12 },
              },
            },
            scales: {
              x: {
                ticks: {
                  color: "#64748b",
                  font: { size: 11 },
                  maxRotation: 0,
                  autoSkip: true,
                  maxTicksLimit: window.innerWidth < 768 ? 5 : 8
                },
                grid: {
                  color: "rgba(148,163,184,0.06)",
                  drawTicks: false,
                },
                border: { display: false },
              },
              y: {
                position: "left",
                ticks: {
                  color: "#64748b",
                  font: { size: 11 },
                  padding: 8,
                  callback: (value) => value.toFixed(0),
                },
                min: yMin,
                max: yMax,
                grid: {
                  color: (ctx) =>
                    baseEquity !== null && Math.abs(ctx.tick.value - baseEquity) < range * 0.01
                      ? "rgba(148,163,184,0.25)"
                      : "rgba(148,163,184,0.06)",
                  drawTicks: false,
                },
                border: { display: false },
                title: {
                  display: true,
                  text: 'USDT',
                  color: '#64748b',
                  font: { size: 10 },
                },
              },
              // 右侧百分比Y轴
              yPct: {
                position: "right",
                display: baseEquity !== null && baseEquity > 0, // 只有有初始资金时才显示
                ticks: {
                  color: "#64748b",
                  font: { size: 11 },
                  padding: 8,
                  callback: function(value) {
                    // 使用闭包捕获的 baseEquity
                    const base = baseEquity;
                    if (base === null || base <= 0) return '';
                    const pct = ((value - base) / base) * 100;
                    return (pct >= 0 ? '+' : '') + pct.toFixed(1) + '%';
                  },
                },
                min: yMin,
                max: yMax,
                grid: {
                  drawOnChartArea: false, // 不绘制网格线，避免重复
                  drawTicks: false,
                },
                border: { display: false },
                title: {
                  display: baseEquity !== null && baseEquity > 0,
                  text: t('dashboard.vsBaseline'),
                  color: '#64748b',
                  font: { size: 10 },
                },
              },
            },
          },
          plugins: [drawdownPlugin],
        });

        this._equityChart.$ddStart = ddStart;
        this._equityChart.$ddEnd = ddEnd;
        this._equityChart.$hasDrawdown = hasDrawdown;
      } finally {
        this._equityRendering = false;
      }
    },
  },

  template: `
<div class="min-h-screen bg-gradient-to-br from-slate-900 via-slate-900 to-slate-800 text-white">
  <!-- 共享导航组件 -->
  <div id="shared-nav"></div>

  <!-- Logic Page (公开模式不显示) -->
  <div v-if="activePage==='logic' && !isPublicMode" class="max-w-7xl mx-auto px-4 py-6 md:py-8">
    <!-- 页面头部 -->
    <div class="mb-6">
      <div class="flex flex-col md:flex-row md:items-center md:justify-between gap-4 mb-4">
        <div>
          <h1 class="text-2xl md:text-3xl font-bold text-white mb-2">{{ t('logic.title') }}</h1>
          <div class="flex items-center gap-3 text-sm text-slate-400">
            <span>{{ t('common.total') }} <span class="text-white font-medium">{{ analysisTotal || analysisList.length }}</span> {{ t('logic.records') }}</span>
            <span class="text-slate-600">·</span>
            <span>{{ t('logic.executedRounds', {count: decisionRounds}) }}</span>
          </div>
        </div>
        
        <div class="flex items-center gap-3">
          <div class="flex items-center gap-2 bg-slate-800/50 rounded-lg p-1 border border-slate-700/50">
            <span class="text-slate-400 text-sm pl-2">{{ t('logic.recent') }}</span>
            <select v-model.number="analysisLimit" @change="loadAnalysisHistory"
                    class="bg-slate-700 text-white px-3 py-1.5 rounded-md border-0 text-sm focus:ring-2 focus:ring-blue-500 outline-none">
              <option :value="8">8 {{ t('logic.records') }}</option>
              <option :value="50">50 {{ t('logic.records') }}</option>
              <option :value="100">100 {{ t('logic.records') }}</option>
            </select>
          </div>
          <button @click="loadAnalysisHistory" :disabled="analysisLoading"
                  class="bg-blue-500 hover:bg-blue-600 px-4 py-2 rounded-lg disabled:opacity-50 text-sm font-medium transition-colors flex items-center gap-2">
            <svg class="w-4 h-4" :class="analysisLoading ? 'animate-spin' : ''" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M4 4v5h.582m15.356 2A8.001 8.001 0 004.582 9m0 0H9m11 11v-5h-.581m0 0a8.003 8.003 0 01-15.357-2m15.357 2H15"/>
            </svg>
            {{ t('common.refresh') }}
          </button>
        </div>
      </div>
    </div>

    <!-- 加载状态 -->
    <div v-if="analysisLoading" class="flex flex-col items-center justify-center py-20">
      <div class="relative">
        <div class="w-12 h-12 border-4 border-slate-700 rounded-full"></div>
        <div class="absolute top-0 left-0 w-12 h-12 border-4 border-blue-500 rounded-full animate-spin border-t-transparent"></div>
      </div>
      <p class="text-slate-400 mt-4 text-sm">{{ t('logic.loadingRecords') }}</p>
    </div>

    <!-- 主体内容 -->
    <div v-else class="grid grid-cols-1 lg:grid-cols-3 gap-4">
      <!-- 左侧列表 -->
      <div class="bg-slate-800/30 rounded-2xl border border-slate-700/50 overflow-hidden">
        <div class="px-4 py-3 border-b border-slate-700/50 bg-slate-800/50">
          <h3 class="text-sm font-medium text-slate-300">{{ t('logic.decisionHistory') }}</h3>
        </div>
        <div class="divide-y divide-slate-700/30 max-h-[500px] lg:max-h-[650px] overflow-y-auto">
          <div v-for="(item, i) in analysisListView" :key="item.id || i"
               @click="isPublicMode ? selectPublicAnalysisItem(i) : selectAnalysisItem(i)"
               :class="i===selectedAnalysisIndex 
                 ? 'bg-blue-500/10 border-l-2 border-l-blue-500' 
                 : 'hover:bg-slate-700/30 border-l-2 border-l-transparent'"
               class="p-4 cursor-pointer transition-all">
            
            <div class="flex justify-between items-start gap-2 mb-2">
              <div class="text-white font-medium">
                {{ t('logic.round', {num: decisionRounds - analysisOffset - i}) }}
              </div>
              <span :class="(item.http_status || item.response?.http_status) === 200 ? 'text-green-400 bg-green-500/10' : 'text-red-400 bg-red-500/10'"
                    class="text-[10px] px-1.5 py-0.5 rounded font-medium">
                {{ item.http_status || item.response?.http_status }}
              </span>
            </div>
            
            <div class="text-xs text-slate-500 mb-2">
              {{ formatUnixSecondsToLocal(item.timestamp || item.response?.timestamp) }}
            </div>
            
            <div class="flex flex-wrap gap-1.5 text-xs">
              <!-- V2 API 摘要字段（登录和公开模式统一使用） -->
              <span class="px-2 py-0.5 rounded bg-slate-700/50 text-slate-300">
                {{ item.signals_count || 0 }} {{ t('logic.signals') }}
              </span>
              <span class="px-2 py-0.5 rounded bg-blue-500/20 text-blue-400">
                {{ item.total_tokens || 0 }} {{ t('logic.tokens') }}
              </span>
              <span class="px-2 py-0.5 rounded bg-purple-500/20 text-purple-400">
                {{ item.response_time_ms || 0 }}ms
              </span>
            </div>
          </div>

          <div v-if="analysisList.length===0" class="text-center py-12 text-slate-500 text-sm">
            {{ t('logic.noRecords') }}
          </div>
        </div>
      </div>

      <!-- 右侧详情 -->
      <div class="lg:col-span-2 space-y-4" v-if="analysisList.length > 0">
        <!-- 详情加载中（V2 API 模式：等待详情加载，登录和公开模式统一）-->
        <div v-if="analysisDetailLoading || !selectedAnalysis" class="bg-slate-800/30 rounded-2xl border border-slate-700/50 p-8 flex flex-col items-center justify-center min-h-[200px]">
          <div class="relative">
            <div class="w-8 h-8 border-3 border-slate-700 rounded-full"></div>
            <div class="absolute top-0 left-0 w-8 h-8 border-3 border-blue-500 rounded-full animate-spin border-t-transparent"></div>
          </div>
          <p class="text-slate-400 mt-3 text-sm">{{ t('logic.loadingDetail') }}</p>
        </div>
        
        <template v-if="selectedAnalysis && !analysisDetailLoading">
        <!-- 决策概览卡片 -->
        <div class="bg-slate-800/30 rounded-2xl border border-slate-700/50 overflow-hidden">
          <div class="px-5 py-4 border-b border-slate-700/50 bg-slate-800/50">
            <div class="flex flex-col sm:flex-row sm:items-center sm:justify-between gap-2">
              <div class="flex items-center gap-3">
                <h3 class="text-white font-semibold">
                  {{ t('logic.round', {num: decisionRounds - analysisOffset - selectedAnalysisIndex}) }}
                </h3>
                <span class="text-xs px-2 py-0.5 rounded-full bg-slate-700 text-slate-300">
                  {{ signalsSummary.total }} {{ t('logic.symbols') }}
                </span>
              </div>

              <div class="flex items-center gap-2 text-xs flex-wrap">
                <span class="flex items-center gap-1.5 px-2 py-1 rounded-lg bg-green-500/10 text-green-400">
                  <span class="w-1.5 h-1.5 rounded-full bg-green-400"></span>
                  {{ signalsSummary.signal }} {{ t('logic.signal') }}
                </span>
                <span class="flex items-center gap-1.5 px-2 py-1 rounded-lg bg-blue-500/10 text-blue-400">
                  <span class="w-1.5 h-1.5 rounded-full bg-blue-400"></span>
                  {{ signalsSummary.hold }} {{ t('logic.hold') }}
                </span>
                <span class="flex items-center gap-1.5 px-2 py-1 rounded-lg bg-yellow-500/10 text-yellow-400">
                  <span class="w-1.5 h-1.5 rounded-full bg-yellow-400"></span>
                  {{ signalsSummary.wait }} {{ t('logic.wait') }}
                </span>
                <span :class="selectedAnalysis.response?.http_status === 200 ? 'bg-green-500/10 text-green-400' : 'bg-red-500/10 text-red-400'"
                      class="px-2 py-1 rounded-lg">
                  {{ t('logic.http') }} {{ selectedAnalysis.response?.http_status ?? '-' }}
                </span>
              </div>
            </div>
          </div>

          <!-- 信号表格 -->
          <div class="overflow-x-auto">
            <div class="max-h-[320px] overflow-auto">
              <table class="min-w-full text-sm">
                <thead class="sticky top-0 bg-slate-900/95 backdrop-blur">
                  <tr class="border-b border-slate-700/50">
                    <th class="py-3 px-4 text-left text-xs font-medium text-slate-400 uppercase tracking-wider">{{ t('dashboard.symbol') }}</th>
                    <th class="py-3 px-4 text-left text-xs font-medium text-slate-400 uppercase tracking-wider">{{ t('logic.action') }}</th>
                    <th class="py-3 px-4 text-right text-xs font-medium text-slate-400 uppercase tracking-wider">{{ t('logic.entry') }}</th>
                    <th class="py-3 px-4 text-right text-xs font-medium text-slate-400 uppercase tracking-wider">{{ t('logic.positionSize') }}</th>
                    <th class="py-3 px-4 text-right text-xs font-medium text-slate-400 uppercase tracking-wider">{{ t('logic.sl') }}</th>
                    <th class="py-3 px-4 text-right text-xs font-medium text-slate-400 uppercase tracking-wider">{{ t('logic.tp') }}</th>
                  </tr>
                </thead>

                <tbody class="divide-y divide-slate-700/30">
                  <tr v-for="(s, i) in normalizedSignals" :key="i" class="hover:bg-slate-700/20 transition-colors">
                    <td class="py-3 px-4 text-white font-medium whitespace-nowrap">
                      {{ s.symbol || 'N/A' }}
                    </td>

                    <td class="py-3 px-4">
                      <span class="px-2.5 py-1 rounded-full text-xs font-semibold whitespace-nowrap"
                        :class="getActionClass(s.action)">
                        {{ formatAction(s.action) }}
                      </span>
                    </td>

                    <td class="py-3 px-4 text-right text-slate-300 font-mono whitespace-nowrap">{{ s.entry ?? '—' }}</td>
                    <td class="py-3 px-4 text-right text-slate-300 font-mono whitespace-nowrap">{{ s.position_size ?? '—' }}</td>
                    <td class="py-3 px-4 text-right text-slate-300 font-mono whitespace-nowrap">{{ s.stop_loss ?? '—' }}</td>
                    <td class="py-3 px-4 text-right text-slate-300 font-mono whitespace-nowrap">{{ s.take_profit ?? '—' }}</td>
                  </tr>

                  <tr v-if="normalizedSignals.length===0">
                    <td colspan="6" class="py-8 text-center text-sm">
                      <template v-if="selectedAnalysis.response?.error">
                        <div class="text-red-400 font-medium mb-2">⚠️ {{ t('logic.llmFailed') }}</div>
                        <div class="text-red-300/80 text-xs max-w-lg mx-auto break-all">
                          {{ selectedAnalysis.response?.error_message || t('logic.unknownError') }}
                        </div>
                        <div v-if="selectedAnalysis.response?.provider || selectedAnalysis.response?.error_code" 
                             class="text-slate-500 text-xs mt-2">
                          <span v-if="selectedAnalysis.response?.provider">{{ t('logic.provider') }}: {{ selectedAnalysis.response.provider }}</span>
                          <span v-if="selectedAnalysis.response?.provider && selectedAnalysis.response?.error_code" class="mx-1">|</span>
                          <span v-if="selectedAnalysis.response?.error_code">{{ t('logic.code') }}: {{ selectedAnalysis.response.error_code }}</span>
                        </div>
                      </template>
                      <template v-else>
                        <span class="text-slate-500">{{ t('logic.noSignals') }}</span>
                      </template>
                    </td>
                  </tr>
                </tbody>
              </table>
            </div>
          </div>
        </div>

        <!-- 折叠面板 -->
        <div class="space-y-3">
          <details class="bg-slate-800/30 rounded-xl border border-slate-700/50 overflow-hidden group">
            <summary class="px-4 py-3 text-slate-300 text-sm cursor-pointer hover:bg-slate-700/30 select-none flex items-center gap-2 transition-colors">
              <svg class="w-4 h-4 text-slate-500 transition-transform group-open:rotate-90" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 5l7 7-7 7"/>
              </svg>
              {{ t('logic.tokenUsage') }}
            </summary>
          
            <div class="px-4 pb-4 space-y-2">
              <div class="grid grid-cols-1 sm:grid-cols-3 gap-2 text-xs">
                <div class="bg-slate-900/40 border border-slate-700/40 rounded-lg p-3">
                  <div class="text-slate-500 mb-1">prompt_tokens</div>
                  <div class="text-slate-200 font-mono">{{ selectedAnalysis.response?.usage?.prompt_tokens ?? '—' }}</div>
                </div>
                <div class="bg-slate-900/40 border border-slate-700/40 rounded-lg p-3">
                  <div class="text-slate-500 mb-1">completion_tokens</div>
                  <div class="text-slate-200 font-mono">{{ selectedAnalysis.response?.usage?.completion_tokens ?? '—' }}</div>
                </div>
                <div class="bg-slate-900/40 border border-slate-700/40 rounded-lg p-3">
                  <div class="text-slate-500 mb-1">total_tokens</div>
                  <div class="text-slate-200 font-mono">{{ selectedAnalysis.response?.usage?.total_tokens ?? '—' }}</div>
                </div>
              </div>
          
              <div class="text-[11px] text-slate-500">
                model: <span class="text-slate-300 font-mono">{{ selectedAnalysis.response?.usage?.model ?? '—' }}</span>
                <span class="mx-2 text-slate-700">|</span>
                finish: <span class="text-slate-300 font-mono">{{ selectedAnalysis.response?.finish_reason ?? '—' }}</span>
                <span class="mx-2 text-slate-700">|</span>
                ms: <span class="text-slate-300 font-mono">{{ selectedAnalysis.response?.response_time_ms ?? '—' }}</span>
              </div>
          
              <!-- 如果你还想看 usage 全量 -->
<!--              <pre class="text-xs text-slate-400 max-h-[220px] overflow-auto whitespace-pre-wrap break-words bg-slate-900/40 border border-slate-700/40 rounded-lg p-3">-->
<!--          {{ JSON.stringify(selectedAnalysis.response?.usage || null, null, 2) }}-->
<!--              </pre>-->
            </div>
          </details>

          <details class="bg-slate-800/30 rounded-xl border border-slate-700/50 overflow-hidden group">
            <summary class="px-4 py-3 text-slate-300 text-sm cursor-pointer hover:bg-slate-700/30 select-none flex items-center gap-2 transition-colors">
              <svg class="w-4 h-4 text-slate-500 transition-transform group-open:rotate-90" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 5l7 7-7 7"/>
              </svg>
              {{ t('logic.viewReason') }}
            </summary>
            <div class="px-4 pb-4 space-y-2 max-h-[300px] overflow-auto">
              <div v-for="(s, i) in normalizedSignals" :key="'r'+i"
                   class="text-xs text-slate-300 bg-slate-900/40 border border-slate-700/40 rounded-lg p-3">
                <div class="flex items-center gap-2 text-slate-200 font-medium mb-2">
                  <span>{{ s.symbol || 'N/A' }}</span>
                  <span class="px-1.5 py-0.5 rounded text-[10px]"
                        :class="{
                          'bg-green-500/20 text-green-400': (s.action||'').startsWith('open'),
                          'bg-red-500/20 text-red-400': (s.action||'').startsWith('close'),
                          'bg-yellow-500/20 text-yellow-300': (s.action||'') === 'wait',
                          'bg-slate-500/20 text-slate-300': !['open','close','wait'].some(a => (s.action||'').includes(a))
                        }">
                    {{ s.action || 'unknown' }}
                  </span>
                </div>
                <div class="whitespace-pre-wrap break-words text-slate-400 leading-relaxed">{{ s.reason || '—' }}</div>
              </div>
            </div>
          </details>

          <details class="bg-slate-800/30 rounded-xl border border-slate-700/50 overflow-hidden group">
            <summary class="px-4 py-3 text-slate-300 text-sm cursor-pointer hover:bg-slate-700/30 select-none flex items-center gap-2 transition-colors">
              <svg class="w-4 h-4 text-slate-500 transition-transform group-open:rotate-90" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 5l7 7-7 7"/>
              </svg>
              {{ t('logic.viewResponse') }}
            </summary>
            <div class="px-4 pb-4">
              <pre class="text-xs text-slate-400 max-h-[260px] overflow-auto whitespace-pre-wrap break-words bg-slate-900/40 border border-slate-700/40 rounded-lg p-3">{{ selectedAnalysis.response?.content }}</pre>
            </div>
          </details>

          <details v-if="formattedSystemPrompt" class="bg-slate-800/30 rounded-xl border border-slate-700/50 overflow-hidden group">
            <summary class="px-4 py-3 text-slate-300 text-sm cursor-pointer hover:bg-slate-700/30 select-none flex items-center gap-2 transition-colors">
              <svg class="w-4 h-4 text-slate-500 transition-transform group-open:rotate-90" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 5l7 7-7 7"/>
              </svg>
              {{ t('logic.viewSystemPrompt') }}
            </summary>
            <div class="px-4 pb-4">
              <pre class="text-xs text-slate-400 max-h-[400px] overflow-auto whitespace-pre-wrap break-words bg-slate-900/40 border border-slate-700/40 rounded-lg p-3">{{ formattedSystemPrompt }}</pre>
            </div>
          </details>

          <details class="bg-slate-800/30 rounded-xl border border-slate-700/50 overflow-hidden group">
            <summary class="px-4 py-3 text-slate-300 text-sm cursor-pointer hover:bg-slate-700/30 select-none flex items-center gap-2 transition-colors">
              <svg class="w-4 h-4 text-slate-500 transition-transform group-open:rotate-90" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 5l7 7-7 7"/>
              </svg>
              {{ t('logic.viewFeedData') }}
            </summary>
            <div class="px-4 pb-4">
              <pre class="text-xs text-slate-400 max-h-[320px] overflow-auto whitespace-pre-wrap break-words bg-slate-900/40 border border-slate-700/40 rounded-lg p-3">{{ JSON.stringify(requestWithoutPrompt, null, 2) }}</pre>
            </div>
          </details>

          <details class="bg-slate-800/30 rounded-xl border border-slate-700/50 overflow-hidden group">
            <summary class="px-4 py-3 text-slate-300 text-sm cursor-pointer hover:bg-slate-700/30 select-none flex items-center gap-2 transition-colors">
              <svg class="w-4 h-4 text-slate-500 transition-transform group-open:rotate-90" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 5l7 7-7 7"/>
              </svg>
              {{ t('logic.viewResponseJson') }}
            </summary>
            <div class="px-4 pb-4">
              <pre class="text-xs text-slate-400 max-h-[320px] overflow-auto whitespace-pre-wrap break-words bg-slate-900/40 border border-slate-700/40 rounded-lg p-3">{{ JSON.stringify(selectedAnalysis.response, null, 2) }}</pre>
            </div>
          </details>
        </div>
        </template>
      </div>
    </div>
  </div>

  <!-- Dashboard Page -->
  <div v-if="activePage==='dashboard'" class="max-w-7xl mx-auto px-4 py-6 md:py-8">
    <!-- Loading State -->
    <div v-show="loading" class="flex flex-col items-center justify-center py-32">
      <div class="relative">
        <div class="w-16 h-16 border-4 border-slate-700 rounded-full"></div>
        <div class="absolute top-0 left-0 w-16 h-16 border-4 border-blue-500 rounded-full animate-spin border-t-transparent"></div>
      </div>
      <p class="text-slate-400 mt-6 text-sm">{{ t('common.loading') }}</p>
    </div>
    
    <!-- Public Mode Error State -->
    <div v-if="isPublicMode && publicError && !loading" class="flex flex-col items-center justify-center py-32">
      <div class="w-20 h-20 rounded-full bg-red-500/20 flex items-center justify-center mb-6">
        <svg class="w-10 h-10 text-red-400" fill="none" stroke="currentColor" viewBox="0 0 24 24">
          <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 9v2m0 4h.01m-6.938 4h13.856c1.54 0 2.502-1.667 1.732-3L13.732 4c-.77-1.333-2.694-1.333-3.464 0L3.34 16c-.77 1.333.192 3 1.732 3z"/>
        </svg>
      </div>
      <h2 class="text-2xl text-white font-bold mb-3">{{ t('error.cannotAccess') }}</h2>
      <p class="text-slate-400 mb-6">{{ publicError }}</p>
      <a href="/" class="px-6 py-3 bg-blue-500 hover:bg-blue-600 text-white rounded-xl text-sm font-medium transition-colors">
        {{ t('error.backHome') }}
      </a>
    </div>

    <div v-show="!loading && !(isPublicMode && publicError)">
      <!-- Header -->
      <div class="mb-6 md:mb-8">
        <div class="flex flex-col gap-4">
          <div>
            <h1 class="text-xl md:text-2xl lg:text-3xl font-bold text-white mb-2">
              <template v-if="isPublicMode && publicDisplayName">
                {{ t('dashboard.publicTitle', { name: publicDisplayName }) }}
              </template>
              <template v-else>
                {{ t('dashboard.title') }}
              </template>
            </h1>
            <div class="flex flex-wrap items-center gap-2 md:gap-3 text-xs md:text-sm">
              <span v-if="isPublicMode" class="inline-flex items-center gap-1.5 px-2 md:px-3 py-1 rounded-full bg-green-500/20 border border-green-500/30 text-green-300">
                <span class="w-1.5 h-1.5 md:w-2 md:h-2 rounded-full bg-green-500"></span>
                {{ t('dashboard.publicMode') }}
              </span>
              <span v-else class="inline-flex items-center gap-1.5 px-2 md:px-3 py-1 rounded-full bg-slate-800 border border-slate-700 text-slate-300">
                <span class="w-1.5 h-1.5 md:w-2 md:h-2 rounded-full bg-green-500 animate-pulse"></span>
                {{ t('dashboard.realtime') }}
              </span>
              <span class="text-slate-500 text-xs md:text-sm">
                {{ t('dashboard.statsRange') }}: {{ statsLimit === -1 ? t('dashboard.allTrades') : t('dashboard.recentTrades', { count: statsLimit }) }}
              </span>
            </div>
          </div>
          
          <!-- 交易所筛选按钮 + 基准资金 + 刷新 -->
          <div class="flex flex-wrap items-center gap-2 md:gap-3">
            <div v-if="enabledExchanges.length > 0" class="flex items-center gap-0.5 md:gap-1 p-0.5 md:p-1 rounded-lg md:rounded-xl bg-slate-800/50 border border-slate-700/50 overflow-x-auto">
              <!-- 全局统计按钮 -->
              <button @click="isPublicMode ? selectPublicExchange(null) : selectExchange(null)"
                 :class="selectedExchange === null 
                   ? 'bg-blue-500 text-white shadow-lg shadow-blue-500/25' 
                   : 'text-slate-400 hover:text-white hover:bg-slate-700/50'"
                 class="px-2 md:px-3 py-1 md:py-1.5 rounded-md md:rounded-lg text-xs md:text-sm font-medium transition-all duration-200 whitespace-nowrap">
                {{ t('dashboard.global') }}
              </button>
              <!-- 各交易所按钮（带连接状态指示器）-->
              <template v-for="ex in enabledExchanges" :key="ex?.exchange || 'ex'">
                <button v-if="ex && ex.exchange"
                   @click="isPublicMode ? selectPublicExchange(ex.exchange) : selectExchange(ex.exchange)"
                   :class="selectedExchange === ex.exchange 
                     ? 'bg-blue-500 text-white shadow-lg shadow-blue-500/25' 
                     : 'text-slate-400 hover:text-white hover:bg-slate-700/50'"
                   class="flex items-center gap-1 md:gap-1.5 px-2 md:px-3 py-1 md:py-1.5 rounded-md md:rounded-lg text-xs md:text-sm font-medium transition-all duration-200 whitespace-nowrap">
                  <span class="relative flex h-1.5 w-1.5 md:h-2 md:w-2">
                    <span v-if="ex.status === 'connected'" class="animate-ping absolute inline-flex h-full w-full rounded-full bg-green-400 opacity-75"></span>
                    <span :class="getConnectionStatusClass(ex.status)" class="relative inline-flex rounded-full h-1.5 w-1.5 md:h-2 md:w-2"></span>
                  </span>
                  <span class="hidden sm:inline">{{ getExchangeDisplayName(ex.exchange) }}</span>
                  <span class="sm:hidden">{{ getExchangeDisplayName(ex.exchange).slice(0, 3) }}</span>
                </button>
              </template>
            </div>
            
            <div v-if="initialEquity !== null" class="hidden sm:flex items-center gap-1.5 md:gap-2 px-2 md:px-4 py-1.5 md:py-2 rounded-lg md:rounded-xl bg-slate-800/50 border border-slate-700/50">
              <span class="text-slate-400 text-xs md:text-sm">{{ t('dashboard.baseline') }}</span>
              <span class="text-white font-semibold text-xs md:text-sm">{{ Number(initialEquity).toFixed(2) }}</span>
              <span class="text-slate-400 text-xs md:text-sm">{{ t('common.usdt') }}</span>
            </div>
            
            <!-- 刷新按钮 -->
            <button @click="isPublicMode ? loadPublicData(true) : loadData(true)"
                    :disabled="loading"
                    class="p-1.5 md:p-2 rounded-lg bg-slate-800/50 border border-slate-700/50 text-slate-400 hover:text-white hover:bg-slate-700/50 transition-all disabled:opacity-50 ml-auto">
              <svg class="w-4 h-4 md:w-5 md:h-5" :class="loading ? 'animate-spin' : ''" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M4 4v5h.582m15.356 2A8.001 8.001 0 004.582 9m0 0H9m11 11v-5h-.581m0 0a8.003 8.003 0 01-15.357-2m15.357 2H15"/>
              </svg>
            </button>
          </div>
        </div>
      </div>

      <!-- Stats Cards Row 1 - 核心指标 -->
      <div class="grid grid-cols-2 lg:grid-cols-4 gap-2 md:gap-4 mb-2 md:mb-4">
        <!-- 总净收益 -->
        <div class="group relative bg-gradient-to-br from-slate-800/80 to-slate-800/40 rounded-xl md:rounded-2xl p-3 md:p-5 border border-slate-700/50 hover:border-slate-600/50 transition-all duration-300 overflow-hidden">
          <div class="absolute top-0 right-0 w-16 md:w-20 h-16 md:h-20 bg-gradient-to-br from-green-500/10 to-transparent rounded-bl-full"></div>
          <div class="relative">
            <div class="flex items-center gap-1.5 md:gap-2 text-slate-400 text-[10px] md:text-xs mb-1 md:mb-2">
              <svg class="w-3 h-3 md:w-4 md:h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 8c-1.657 0-3 .895-3 2s1.343 2 3 2 3 .895 3 2-1.343 2-3 2m0-8c1.11 0 2.08.402 2.599 1M12 8V7m0 1v8m0 0v1m0-1c-1.11 0-2.08-.402-2.599-1M21 12a9 9 0 11-18 0 9 9 0 0118 0z"/></svg>
              {{ t('dashboard.totalPnl') }}
            </div>
            <div :class="statistics.totalPnl >= 0 ? 'text-green-400' : 'text-red-400'"
                 class="text-lg md:text-2xl lg:text-3xl font-bold tracking-tight">
              {{ statistics.totalPnl >= 0 ? '+' : '' }}{{ Number(statistics.totalPnl).toFixed(2) }}
            </div>
            <div class="flex items-center gap-1 md:gap-2 mt-1 md:mt-2">
              <span :class="totalReturnPct >= 0 ? 'bg-green-500/20 text-green-400' : 'bg-red-500/20 text-red-400'"
                    class="inline-flex items-center gap-0.5 md:gap-1 px-1.5 md:px-2 py-0.5 rounded-full text-[10px] md:text-xs font-medium">
                <svg :class="totalReturnPct >= 0 ? '' : 'rotate-180'" class="w-2.5 h-2.5 md:w-3 md:h-3" fill="currentColor" viewBox="0 0 20 20"><path fill-rule="evenodd" d="M5.293 9.707a1 1 0 010-1.414l4-4a1 1 0 011.414 0l4 4a1 1 0 01-1.414 1.414L11 7.414V15a1 1 0 11-2 0V7.414L6.707 9.707a1 1 0 01-1.414 0z" clip-rule="evenodd"/></svg>
                {{ totalReturnPct >= 0 ? '+' : '' }}{{ totalReturnPct.toFixed(2) }}%
              </span>
              <span class="hidden md:inline text-slate-500 text-xs">USDT</span>
            </div>
          </div>
        </div>

        <!-- 持仓实时净收益 -->
        <div class="group relative bg-gradient-to-br from-slate-800/80 to-slate-800/40 rounded-xl md:rounded-2xl p-3 md:p-5 border border-slate-700/50 hover:border-slate-600/50 transition-all duration-300 overflow-hidden">
          <div class="absolute top-0 right-0 w-16 md:w-20 h-16 md:h-20 bg-gradient-to-br from-orange-500/10 to-transparent rounded-bl-full"></div>
          <div class="relative">
            <div class="flex items-center gap-1.5 md:gap-2 text-slate-400 text-[10px] md:text-xs mb-1 md:mb-2">
              <svg class="w-3 h-3 md:w-4 md:h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M13 7h8m0 0v8m0-8l-8 8-4-4-6 6"/></svg>
              <span class="hidden sm:inline">{{ t('dashboard.unrealizedPnl') }}</span>
              <span class="sm:hidden">{{ t('dashboard.unrealizedPnl') }}</span>
            </div>
            <div :class="totalLiveNetPnl >= 0 ? 'text-green-400' : 'text-red-400'" class="text-lg md:text-2xl lg:text-3xl font-bold tracking-tight">
              {{ totalLiveNetPnl >= 0 ? '+' : '' }}{{ totalLiveNetPnl.toFixed(2) }}
            </div>
            <div class="flex items-center gap-1 md:gap-2 mt-1 md:mt-2 text-[10px] md:text-xs text-slate-500">
              <span class="hidden sm:inline">{{ t('dashboard.avgHoldTime') }}</span>
              <span class="sm:hidden">{{ t('dashboard.avgHoldTime') }}</span>
              <span class="text-slate-300 font-medium">{{ formatDuration(statistics.avgDuration * 60000) }}</span>
            </div>
          </div>
        </div>

        <!-- 胜率 -->
        <div class="group relative bg-gradient-to-br from-slate-800/80 to-slate-800/40 rounded-xl md:rounded-2xl p-3 md:p-5 border border-slate-700/50 hover:border-slate-600/50 transition-all duration-300 overflow-hidden">
          <div class="absolute top-0 right-0 w-16 md:w-20 h-16 md:h-20 bg-gradient-to-br from-purple-500/10 to-transparent rounded-bl-full"></div>
          <div class="relative">
            <div class="flex items-center gap-1.5 md:gap-2 text-slate-400 text-[10px] md:text-xs mb-1 md:mb-2">
              <svg class="w-3 h-3 md:w-4 md:h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 19v-6a2 2 0 00-2-2H5a2 2 0 00-2 2v6a2 2 0 002 2h2a2 2 0 002-2zm0 0V9a2 2 0 012-2h2a2 2 0 012 2v10m-6 0a2 2 0 002 2h2a2 2 0 002-2m0 0V5a2 2 0 012-2h2a2 2 0 012 2v14a2 2 0 01-2 2h-2a2 2 0 01-2-2z"/></svg>
              {{ t('dashboard.winRate') }}
            </div>
            <div class="text-lg md:text-2xl lg:text-3xl font-bold tracking-tight text-purple-400">
              {{ statistics.winRate }}<span class="text-sm md:text-lg">%</span>
            </div>
            <div class="mt-1 md:mt-2 space-y-0.5 md:space-y-1 text-[10px] md:text-xs">
              <div class="flex items-center gap-1 md:gap-2">
                <span class="text-green-400 font-medium">{{ statistics.winCount }} {{ t('dashboard.win') }}</span>
                <span class="hidden sm:inline text-slate-500">({{ t('dashboard.long') }} {{ statistics.longWins }} / {{ t('dashboard.short') }} {{ statistics.shortWins }})</span>
              </div>
              <div class="flex items-center gap-1 md:gap-2">
                <span class="text-red-400 font-medium">{{ statistics.lossCount ?? 0 }} {{ t('dashboard.loss') }}</span>
                <span class="hidden sm:inline text-slate-500">({{ t('dashboard.long') }} {{ statistics.longLosses ?? 0 }} / {{ t('dashboard.short') }} {{ statistics.shortLosses ?? 0 }})</span>
              </div>
            </div>
          </div>
        </div>

        <!-- 交易次数 -->
        <div class="group relative bg-gradient-to-br from-slate-800/80 to-slate-800/40 rounded-xl md:rounded-2xl p-3 md:p-5 border border-slate-700/50 hover:border-slate-600/50 transition-all duration-300 overflow-hidden">
          <div class="absolute top-0 right-0 w-16 md:w-20 h-16 md:h-20 bg-gradient-to-br from-blue-500/10 to-transparent rounded-bl-full"></div>
          <div class="relative">
            <div class="flex items-center gap-1.5 md:gap-2 text-slate-400 text-[10px] md:text-xs mb-1 md:mb-2">
              <svg class="w-3 h-3 md:w-4 md:h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M7 12l3-3 3 3 4-4M8 21l4-4 4 4M3 4h18M4 4h16v12a1 1 0 01-1 1H5a1 1 0 01-1-1V4z"/></svg>
              {{ t('dashboard.tradeCount') }}
            </div>
            <div class="text-lg md:text-2xl lg:text-3xl font-bold tracking-tight text-blue-400">
              {{ parseInt(statistics.totalTrades) }}
            </div>
            <div class="flex items-center gap-1 md:gap-2 mt-1 md:mt-2 text-[10px] md:text-xs">
              <span class="inline-flex items-center gap-0.5 md:gap-1 px-1.5 md:px-2 py-0.5 rounded-full bg-blue-500/20 text-blue-300">
                <span class="w-1 h-1 md:w-1.5 md:h-1.5 rounded-full bg-blue-400"></span>
                {{ parseInt(statistics.activePositions) }} {{ t('dashboard.inPosition') }}
              </span>
            </div>
          </div>
        </div>
      </div>

      <!-- Stats Cards Row 2 - 详细指标 -->
      <div class="grid grid-cols-2 lg:grid-cols-4 gap-3 md:gap-4 mb-6">
        <!-- 当前账户余额 -->
        <div class="bg-slate-800/30 rounded-xl p-4 border border-slate-700/30 hover:bg-slate-800/50 transition-colors">
          <div class="text-slate-500 text-xs mb-1.5">{{ t('dashboard.accountBalance') }}</div>
          <div class="text-lg font-semibold text-white">
            {{ account && account.walletBalance != null
                ? Number(account.walletBalance).toFixed(2)
                : (Number.isFinite(Number(statistics.walletBalance)) ? Number(statistics.walletBalance).toFixed(2) : '—') }}
            <span class="text-xs text-slate-500 font-normal">USDT</span>
          </div>
        </div>
      
        <!-- 累计交易成本 -->
        <!-- 交易成本 = 手续费 - 资金费收入（资金费正数表示收入，减少成本）-->
        <div class="bg-slate-800/30 rounded-xl p-4 border border-slate-700/30 hover:bg-slate-800/50 transition-colors">
          <div class="text-slate-500 text-xs mb-1.5">{{ t('dashboard.tradingCost') }}</div>
          <div class="text-lg font-semibold"
               :class="(statistics.totalFee - statistics.totalFunding) <= 0 ? 'text-green-400' : 'text-red-400'">
            {{ (statistics.totalFee - statistics.totalFunding) > 0 ? '-' : '+' }}{{ Math.abs(statistics.totalFee - statistics.totalFunding).toFixed(2) }}
            <span class="text-xs text-slate-500 font-normal">USDT</span>
          </div>
          <div class="text-[11px] text-slate-500 mt-1">
            {{ t('dashboard.fee') }} {{ Number(statistics.totalFee || 0).toFixed(2) }} · {{ t('dashboard.funding') }} 
            <span :class="statistics.totalFunding >= 0 ? 'text-green-400/80' : 'text-red-400/80'">
              {{ statistics.totalFunding >= 0 ? '+' : '' }}{{ Number(statistics.totalFunding || 0).toFixed(2) }}
            </span>
          </div>
        </div>
      
        <!-- 收益 / 回撤 -->
        <div class="bg-slate-800/30 rounded-xl p-4 border border-slate-700/30 hover:bg-slate-800/50 transition-colors">
          <div class="text-slate-500 text-xs mb-1.5">{{ t('dashboard.returnDrawdown') }}</div>
          <div class="text-lg font-semibold text-white" v-if="initialEquity !== null">
            <span :class="totalReturnPct >= 0 ? 'text-green-400' : 'text-red-400'">
              {{ totalReturnPct >= 0 ? '+' : '' }}{{ totalReturnPct.toFixed(2) }}%
            </span>
            <span class="text-slate-500 mx-1">/</span>
            <span v-if="maxDrawdownPeakPct === null" class="text-slate-500">—</span>
            <span v-else class="text-red-400">-{{ Math.abs(maxDrawdownPeakPctSafe).toFixed(2) }}%</span>
          </div>
          <div class="text-[11px] text-slate-500 mt-1">
            {{ t('dashboard.calmar') }}: <span class="text-slate-300">{{ calmarRatio != null ? calmarRatio.toFixed(2) : '—' }}</span>
          </div>
        </div>
      
        <!-- 最大回撤 -->
        <div class="bg-slate-800/30 rounded-xl p-4 border border-slate-700/30 hover:bg-slate-800/50 transition-colors">
          <div class="text-slate-500 text-xs mb-1.5">{{ t('dashboard.maxDrawdown') }}</div>
          <div v-if="maxDrawdownPeakPct === null || maxDrawdownPeakAmount === null" class="text-slate-500 text-sm">
            {{ t('dashboard.noDataYet') }}
          </div>
          <div v-else>
            <div class="flex items-baseline gap-2">
              <span class="text-lg font-semibold text-red-400">
                -{{ Math.abs(Number(maxDrawdownPeakAmount)).toFixed(2) }}
              </span>
              <span class="text-xs text-red-400/80">
                -{{ Math.abs(Number(maxDrawdownPeakPct)).toFixed(2) }}%
              </span>
            </div>
            <div class="text-[11px] text-slate-500 mt-1 truncate" 
                 v-if="maxDrawdownPeakFrom && maxDrawdownPeakTo"
                 :title="formatTime(maxDrawdownPeakFrom) + ' → ' + formatTime(maxDrawdownPeakTo)">
              {{ formatTime(maxDrawdownPeakFrom) }} → {{ formatTime(maxDrawdownPeakTo) }}
            </div>
          </div>
        </div>
      </div>

      <!-- Equity Curve -->
      <div class="bg-slate-800/50 rounded-xl p-4 md:p-6 mb-4 md:mb-6 border border-slate-700/50">
        <!-- 头部：标题 + 控制区 -->
        <div class="flex flex-col sm:flex-row sm:items-center sm:justify-between gap-3 mb-4">
          <div class="flex items-center gap-3">
            <h3 class="text-white font-semibold text-base md:text-lg">{{ t('dashboard.equityCurve') }}</h3>
            <span v-if="initialEquity !== null" class="text-xs text-slate-400 bg-slate-700/50 px-2 py-1 rounded">
              {{ t('dashboard.baseline') }} {{ Number(initialEquity).toFixed(2) }} {{ t('common.usdt') }}
            </span>
          </div>
      
          <div class="flex items-center gap-3">
            <!-- 周期按钮组 -->
            <div class="inline-flex rounded-lg bg-slate-900/50 p-0.5 border border-slate-700/50">
              <button
                v-for="tf in ['15m','1D','1M','3M','1Y']"
                :key="tf"
                @click="setEquityAgg(tf)"
                :class="equityAgg===tf 
                  ? 'bg-blue-500 text-white shadow-lg shadow-blue-500/25' 
                  : 'text-slate-400 hover:text-white hover:bg-slate-700/50'"
                class="px-3 py-1.5 text-xs font-medium rounded-md transition-all duration-200"
              >
                {{ tf }}
              </button>
            </div>
            
            <!-- 当前聚合说明 -->
            <span class="hidden sm:inline text-xs text-slate-500">
              {{ equityAgg }} {{ t('dashboard.aggregation') }}
            </span>
          </div>
        </div>
      
        <!-- 图表区域 -->
        <div class="relative">
          <div class="h-[220px] md:h-[300px] lg:h-[350px]">
            <canvas id="equityChart"></canvas>
          </div>
          
          <!-- 空状态 -->
          <div v-if="equityCurveView.length === 0" 
               class="absolute inset-0 flex items-center justify-center bg-slate-800/80 rounded-lg">
            <div class="text-center">
              <div class="text-slate-500 text-sm">{{ t('dashboard.noTradeData') }}</div>
              <div class="text-slate-600 text-xs mt-1">{{ t('dashboard.showAfterTrade') }}</div>
            </div>
          </div>
        </div>
        
        <!-- 底部统计信息 -->
        <div v-if="equityCurveView.length > 0" class="flex flex-wrap items-center gap-4 mt-4 pt-4 border-t border-slate-700/50">
          <div class="flex items-center gap-2">
            <span class="w-3 h-0.5 bg-blue-400 rounded"></span>
            <span class="text-xs text-slate-400">{{ t('dashboard.equityCurve') }}</span>
          </div>
          <div v-if="initialEquity !== null" class="flex items-center gap-2">
            <span class="w-3 h-0.5 bg-slate-500 border-dashed border-t border-slate-400"></span>
            <span class="text-xs text-slate-400">{{ t('dashboard.initialCapital') }}</span>
          </div>
          <div class="flex items-center gap-2">
            <span class="w-3 h-3 bg-red-500/20 border border-red-500/50 rounded-sm"></span>
            <span class="text-xs text-slate-400">{{ t('dashboard.maxDrawdownRange') }}</span>
          </div>
          <div class="ml-auto text-xs text-slate-500">
            {{ t('dashboard.dataPoints', { count: equityCurveView.length }) }}
          </div>
        </div>
      </div>

      <!-- Single Exchange Mode Header -->
      <div v-if="singleExchangeMode" class="flex items-center gap-3 mb-4 p-3 rounded-xl bg-slate-800/50 border border-slate-700/50">
        <div :class="getExchangeColorClass(singleExchangeMode)" class="px-3 py-1.5 rounded-lg text-sm font-semibold">
          {{ getExchangeDisplayName(singleExchangeMode) }}
        </div>
        <div class="flex items-center gap-2">
          <span :class="['w-2 h-2 rounded-full', getConnectionStatusClass(getExchangeStatus(singleExchangeMode))]"></span>
          <span class="text-xs text-slate-400">{{ getConnectionStatusText(getExchangeStatus(singleExchangeMode)) }}</span>
        </div>
        <div class="flex-1"></div>
        <a href="/#dashboard" class="text-xs text-blue-400 hover:text-blue-300 flex items-center gap-1">
          <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M11 17l-5-5m0 0l5-5m-5 5h12"/>
          </svg>
          {{ t('dashboard.backToAll') }}
        </a>
      </div>
      
      <!-- Exchange Filter (show if multiple exchanges in data or enabled) -->
      <div v-if="showExchangeFilter" class="flex flex-wrap items-center gap-2 mb-4">
        <span class="text-sm text-slate-400">{{ t('dashboard.exchangeFilter') }}:</span>
        <button @click="selectExchange(null)"
                :class="selectedExchange === null 
                  ? 'bg-blue-500 text-white' 
                  : 'bg-slate-700/50 text-slate-300 hover:bg-slate-700'"
                class="px-3 py-1.5 rounded-lg text-xs font-medium transition-colors">
          {{ t('dashboard.globalStats') }}
        </button>
        <template v-for="ex in filterableExchanges" :key="ex?.exchange || 'ex-filter'">
          <button v-if="ex && ex.exchange"
                  @click="selectExchange(ex.exchange)"
                  :class="selectedExchange === ex.exchange 
                    ? 'bg-blue-500 text-white' 
                    : 'bg-slate-700/50 text-slate-300 hover:bg-slate-700'"
                  class="px-3 py-1.5 rounded-lg text-xs font-medium transition-colors">
            {{ getExchangeDisplayName(ex.exchange) }}
          </button>
        </template>
        <div class="flex-1"></div>
        <button @click="loadExchangeData" 
                class="text-xs text-slate-400 hover:text-white flex items-center gap-1 transition-colors">
          <svg class="w-3.5 h-3.5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M4 4v5h.582m15.356 2A8.001 8.001 0 004.582 9m0 0H9m11 11v-5h-.581m0 0a8.003 8.003 0 01-15.357-2m15.357 2H15"/>
          </svg>
          {{ t('common.refresh') }}
        </button>
      </div>

      <!-- Exchange Summary Cards (when multiple exchanges in data and exchangeData loaded) -->
      <div v-if="showExchangeFilter && Object.keys(exchangeData).length > 0" class="grid grid-cols-2 lg:grid-cols-4 gap-3 md:gap-4 mb-4">
        <template v-for="ex in filterableExchanges" :key="'summary-' + (ex?.exchange || 'ex')">
          <div v-if="ex && ex.exchange && exchangeData[ex.exchange]"
               @click="selectExchange(ex.exchange)"
               :class="[
                 selectedExchange === ex.exchange ? 'ring-2 ring-blue-500 ring-offset-2 ring-offset-slate-900' : '',
                 getExchangeBgClass(ex.exchange)
               ]"
               class="cursor-pointer rounded-xl p-4 border transition-all hover:scale-[1.02]">
            <div class="flex items-center justify-between mb-2">
              <span class="text-xs font-medium" :class="getExchangeTextClass(ex.exchange)">
                {{ getExchangeDisplayName(ex.exchange) }}
              </span>
              <span class="text-[10px] px-1.5 py-0.5 rounded" :class="getExchangeColorClass(ex.exchange)">
                {{ getExchangePositionCount(ex.exchange) }} 持仓
              </span>
            </div>
            <div class="text-lg font-semibold text-white">
              {{ getExchangeBalance(ex.exchange) }}
              <span class="text-xs text-slate-500 font-normal">USDT</span>
            </div>
            <div class="text-xs mt-1" 
                 :class="getExchangeUnrealizedPnl(ex.exchange) >= 0 ? 'text-green-400' : 'text-red-400'">
              {{ getExchangeUnrealizedPnl(ex.exchange) >= 0 ? '+' : '' }}{{ getExchangeUnrealizedPnl(ex.exchange).toFixed(2) }} {{ t('dashboard.unrealizedPnl') }}
            </div>
          </div>
        </template>
      </div>

      <!-- Tabs -->
      <div class="flex gap-1 mb-4 md:mb-6 bg-slate-800/30 p-1 rounded-xl border border-slate-700/50">
        <button @click="activeTab='positions'"
                :class="activeTab==='positions'
                  ? 'bg-blue-500 text-white shadow-lg shadow-blue-500/20' 
                  : 'text-slate-400 hover:text-white hover:bg-slate-700/30'"
                class="flex-1 py-2.5 md:py-3 rounded-lg text-sm font-medium transition-all duration-200 flex items-center justify-center gap-2">
          <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M13 7h8m0 0v8m0-8l-8 8-4-4-6 6"/></svg>
          {{ t('dashboard.positions') }}
          <span :class="activeTab==='positions' ? 'bg-white/20' : 'bg-slate-700'" 
                class="px-2 py-0.5 rounded-full text-xs">{{ filteredPositions.length }}</span>
        </button>
        <button @click="activeTab='closed'"
                :class="activeTab==='closed'
                  ? 'bg-blue-500 text-white shadow-lg shadow-blue-500/20' 
                  : 'text-slate-400 hover:text-white hover:bg-slate-700/30'"
                class="flex-1 py-2.5 md:py-3 rounded-lg text-sm font-medium transition-all duration-200 flex items-center justify-center gap-2">
          <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 5H7a2 2 0 00-2 2v12a2 2 0 002 2h10a2 2 0 002-2V7a2 2 0 00-2-2h-2M9 5a2 2 0 002 2h2a2 2 0 002-2M9 5a2 2 0 012-2h2a2 2 0 012 2m-6 9l2 2 4-4"/></svg>
          {{ t('dashboard.closedTrades') }}
          <span :class="activeTab==='closed' ? 'bg-white/20' : 'bg-slate-700'" 
                class="px-2 py-0.5 rounded-full text-xs">{{ parseInt(statistics.totalTrades) }}</span>
        </button>
      </div>

      <!-- Positions Table -->
      <div v-show="activeTab === 'positions'" class="bg-slate-800/30 backdrop-blur rounded-2xl border border-slate-700/50 overflow-hidden">
        <div class="overflow-x-auto">
          <table class="min-w-full tabular-nums text-xs md:text-sm">
            <thead class="bg-slate-900/50">
              <tr>
                <th class="px-2 md:px-4 py-3 md:py-4 text-left font-medium text-slate-400 whitespace-nowrap">{{ t('dashboard.symbol') }}</th>
                <th class="px-2 md:px-4 py-3 md:py-4 text-center font-medium text-slate-400 whitespace-nowrap">{{ t('dashboard.side') }}</th>
                <th class="hidden sm:table-cell px-2 md:px-4 py-3 md:py-4 text-right font-medium text-slate-400 whitespace-nowrap">{{ t('dashboard.size') }}</th>
                <th class="px-2 md:px-4 py-3 md:py-4 text-right font-medium text-slate-400 whitespace-nowrap">{{ t('dashboard.entryPrice') }}</th>
                <th class="hidden lg:table-cell px-2 md:px-4 py-3 md:py-4 text-center font-medium text-slate-400 whitespace-nowrap">{{ t('dashboard.orderType') }}</th>
                <th class="hidden xl:table-cell px-2 md:px-4 py-3 md:py-4 text-right font-medium text-slate-400 whitespace-nowrap">{{ t('dashboard.breakEvenPrice') }}</th>
                <th class="px-2 md:px-4 py-3 md:py-4 text-right font-medium text-slate-400 whitespace-nowrap">{{ t('dashboard.pnl') }}</th>
                <th class="hidden md:table-cell px-2 md:px-4 py-3 md:py-4 text-right font-medium text-slate-400 whitespace-nowrap">{{ t('dashboard.stopLoss') }}/{{ t('dashboard.takeProfit') }}</th>
                <th class="hidden lg:table-cell px-2 md:px-4 py-3 md:py-4 text-center font-medium text-slate-400 whitespace-nowrap">{{ t('dashboard.orderType') }}</th>
                <th class="hidden sm:table-cell px-2 md:px-4 py-3 md:py-4 text-right font-medium text-slate-400 whitespace-nowrap">{{ t('dashboard.openTime') }}</th>
              </tr>
            </thead>
            <tbody class="divide-y divide-slate-700/30">
              <tr v-for="pos in filteredPositions" :key="(pos.exchange || 'binance') + pos.symbol + pos.side" class="hover:bg-slate-700/20 transition-colors">
                <td class="px-2 md:px-4 py-3 md:py-4 font-medium text-white whitespace-nowrap text-left">
                  <div class="flex items-center gap-1 md:gap-2">
                    <span class="text-xs md:text-sm">{{ pos.symbol }}</span>
                    <span v-if="pos.exchange && enabledExchanges.length > 1" 
                          :class="getExchangeColorClass(pos.exchange)"
                          class="hidden sm:inline-flex items-center px-1 py-0.5 rounded text-[9px] md:text-[10px] font-medium border">
                      {{ getExchangeDisplayName(pos.exchange) }}
                    </span>
                  </div>
                  <!-- 移动端显示更多信息 -->
                  <div class="sm:hidden text-[10px] text-slate-500 mt-0.5">
                    {{ formatQtyWithPrecision(pos.qty, pos.symbol, pos.exchange) }} · {{ formatTime(pos.openTimeMs) }}
                  </div>
                </td>
                <td class="px-2 md:px-4 py-3 md:py-4 text-center">
                  <span :class="pos.side === 'LONG' ? 'bg-green-500/20 text-green-400 border-green-500/30' : 'bg-red-500/20 text-red-400 border-red-500/30'"
                        class="inline-flex items-center justify-center px-1.5 md:px-3 py-0.5 md:py-1 rounded-full text-[10px] md:text-xs font-semibold border whitespace-nowrap">
                    {{ pos.side }}
                  </span>
                </td>
                <td class="hidden sm:table-cell px-2 md:px-4 py-3 md:py-4 text-right text-slate-300 whitespace-nowrap">
                  {{ formatQtyWithPrecision(pos.qty, pos.symbol, pos.exchange) }}
                </td>
                <td class="px-2 md:px-4 py-3 md:py-4 text-right text-slate-300 whitespace-nowrap font-mono text-[11px] md:text-sm">
                  {{ formatPriceWithPrecision(pos.entryPrice, pos.symbol, pos.exchange) }}
                </td>
                <td class="hidden lg:table-cell px-2 md:px-4 py-3 md:py-4 text-center text-slate-300 whitespace-nowrap">
                  {{ orderTypeLabel(pos.openOrderType) }}
                </td>                
                <td class="hidden xl:table-cell px-2 md:px-4 py-3 md:py-4 text-right text-slate-300 whitespace-nowrap font-mono">
                  {{ formatPriceWithPrecision(pos.breakEvenPrice, pos.symbol, pos.exchange) }}
                </td>
                <td
                  :class="getLivePnl(pos) >= 0 ? 'text-green-400' : 'text-red-400'"
                  class="px-2 md:px-4 py-3 md:py-4 text-right font-semibold whitespace-nowrap font-mono text-[11px] md:text-sm"
                >
                  {{ getLivePnl(pos) >= 0 ? '+' : '' }}{{ formatPrice(getLivePnl(pos)) }}
                </td>
                <td class="hidden md:table-cell px-2 md:px-4 py-3 md:py-4 text-slate-400 text-xs whitespace-nowrap text-right">
                  <div class="font-mono">TP: {{ pos.takeProfitPrice ? formatPriceWithPrecision(pos.takeProfitPrice, pos.symbol, pos.exchange) : '-' }}</div>
                  <div class="font-mono">SL: {{ pos.stopLossPrice ? formatPriceWithPrecision(pos.stopLossPrice, pos.symbol, pos.exchange) : '-' }}</div>
                </td>
                <td class="hidden lg:table-cell px-2 md:px-4 py-3 md:py-4 text-slate-400 capitalize whitespace-nowrap text-center">
                  {{ pos.marginType }}
                </td>
                <td class="hidden sm:table-cell px-2 md:px-4 py-3 md:py-4 text-slate-400 whitespace-nowrap text-right text-xs">
                  {{ formatTime(pos.openTimeMs) }}
                </td>
              </tr>
            </tbody>
          </table>
        </div>
        <div v-if="filteredPositions.length === 0" class="text-center py-12 text-slate-400 text-sm">
          {{ t('dashboard.noPositions') }}
        </div>
      </div>

      <!-- Closed Trades Table -->
      <div v-show="activeTab === 'closed'" class="bg-slate-800/30 backdrop-blur rounded-2xl border border-slate-700/50 overflow-hidden">
        <!-- 分页头部 -->
        <div class="flex flex-col sm:flex-row sm:items-center sm:justify-between gap-2 sm:gap-3 px-3 md:px-5 py-2.5 md:py-3 border-b border-slate-700/50 bg-slate-800/50">
          <div class="flex items-center gap-2 sm:gap-3 text-xs sm:text-sm flex-wrap">
            <span class="text-slate-400">
              {{ t('dashboard.page', { current: Math.floor(closedOffset/closedLimit) + 1 }) }}
            </span>
            <span class="hidden sm:inline text-slate-600">·</span>
            <span class="hidden sm:inline text-slate-400">
              {{ closedLimit }} {{ t('logic.records') }}
            </span>
            <span v-if="closedTotal" class="text-slate-600">·</span>
            <span v-if="closedTotal" class="text-slate-400">
              {{ t('common.total') }} <span class="text-white font-medium">{{ closedTotal }}</span> {{ t('logic.records') }}
            </span>
          </div>
          <div class="flex gap-2">
            <button @click="isPublicMode ? publicPrevPage() : prevClosedPage()" :disabled="!closedHasPrev"
                    class="px-3 md:px-4 py-1.5 md:py-2 rounded-lg border border-slate-600 text-slate-300 hover:bg-slate-700/50 disabled:opacity-40 disabled:hover:bg-transparent text-xs md:text-sm transition-colors flex items-center gap-1">
              <svg class="w-3.5 h-3.5 md:w-4 md:h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M15 19l-7-7 7-7"/></svg>
              <span class="hidden sm:inline">{{ t('dashboard.prevPage') }}</span>
              <span class="sm:hidden">{{ t('dashboard.prevPage') }}</span>
            </button>
            <button @click="isPublicMode ? publicNextPage() : nextClosedPage()" :disabled="!closedHasNext"
                    class="px-3 md:px-4 py-1.5 md:py-2 rounded-lg bg-blue-500 hover:bg-blue-600 text-white disabled:opacity-40 disabled:hover:bg-blue-500 disabled:cursor-not-allowed text-xs md:text-sm transition-colors flex items-center gap-1">
              <span class="hidden sm:inline">{{ t('dashboard.nextPage') }}</span>
              <span class="sm:hidden">{{ t('dashboard.nextPage') }}</span>
              <svg class="w-3.5 h-3.5 md:w-4 md:h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 5l7 7-7 7"/></svg>
            </button>
          </div>
        </div>
        
        <div class="overflow-x-auto">
          <table class="min-w-full text-xs md:text-sm">
            <thead class="bg-slate-900/50">
              <tr>
                <th class="px-2 md:px-4 py-3 text-left font-medium text-slate-400 whitespace-nowrap max-w-[120px] overflow-hidden text-ellipsis" :title="t('dashboard.symbol')">{{ t('dashboard.symbol') }}</th>
                <th class="px-2 md:px-4 py-3 text-center font-medium text-slate-400 whitespace-nowrap">{{ t('dashboard.side') }}</th>
                <th class="hidden sm:table-cell px-2 md:px-4 py-3 text-right font-medium text-slate-400 whitespace-nowrap max-w-[100px] overflow-hidden text-ellipsis" :title="t('dashboard.entryPrice') + '/' + t('dashboard.closePrice')">{{ t('dashboard.price') }}</th>
                <th class="px-2 md:px-4 py-3 text-right font-medium text-slate-400 whitespace-nowrap max-w-[100px] overflow-hidden text-ellipsis" :title="t('dashboard.netPnl') + ' / ' + t('dashboard.peakPnl')">{{ t('dashboard.pnl') }}</th>
                <th class="hidden md:table-cell px-2 md:px-4 py-3 text-right font-medium text-slate-400 whitespace-nowrap max-w-[80px] overflow-hidden text-ellipsis" :title="t('dashboard.maxDrawdown')">{{ t('dashboard.drawdown') }}</th>
                <th class="hidden lg:table-cell px-2 md:px-4 py-3 text-center font-medium text-slate-400 whitespace-nowrap">{{ t('dashboard.duration') }}</th>
                <th class="hidden xl:table-cell px-2 md:px-4 py-3 text-right font-medium text-slate-400 whitespace-nowrap max-w-[60px] overflow-hidden text-ellipsis" :title="t('dashboard.fee')">{{ t('dashboard.fee') }}</th>
                <th class="hidden sm:table-cell px-2 md:px-4 py-3 text-right font-medium text-slate-400 whitespace-nowrap max-w-[100px] overflow-hidden text-ellipsis" :title="t('dashboard.openTime') + '/' + t('dashboard.closeTime')">{{ t('dashboard.openTime') }}</th>
              </tr>
            </thead>
            
            <tbody class="divide-y divide-slate-700/50">
              <tr
                v-for="trade in filteredClosedTrades"
                :key="trade.cycleId"
                class="hover:bg-slate-700/30 transition-colors"
              >
                <!-- 交易对 + 数量 -->
                <td class="px-2 md:px-4 py-2.5 md:py-3 font-medium text-white whitespace-nowrap text-left">
                  <div class="flex items-center gap-1 md:gap-2">
                    <span class="text-xs md:text-sm">{{ trade.symbol }}</span>
                    <span v-if="trade.exchange && enabledExchanges.length > 1" 
                          :class="getExchangeColorClass(trade.exchange)"
                          class="hidden sm:inline-flex items-center px-1 py-0.5 rounded text-[9px] md:text-[10px] font-medium border">
                      {{ getExchangeDisplayName(trade.exchange) }}
                    </span>
                  </div>
                  <!-- 数量显示在下方 -->
                  <div class="text-[10px] text-slate-500 mt-0.5 font-mono tabular-nums">
                    {{ formatQtyWithPrecision(trade.maxAbsQty || trade.openQty, trade.symbol, trade.exchange) }}
                  </div>
                  <!-- 移动端显示价格和时间 -->
                  <div class="sm:hidden text-[10px] text-slate-500 mt-0.5">
                    {{ formatPriceWithPrecision(trade.avgOpenPrice, trade.symbol, trade.exchange) }} → {{ formatPriceWithPrecision(trade.avgClosePrice, trade.symbol, trade.exchange) }}
                  </div>
                </td>
            
                <!-- 方向 -->
                <td class="px-2 md:px-4 py-2.5 md:py-3 whitespace-nowrap text-center">
                  <span
                    :class="trade.side === 'LONG'
                      ? 'bg-green-500/20 text-green-400 border-green-500/30'
                      : 'bg-red-500/20 text-red-400 border-red-500/30'"
                    class="inline-flex items-center justify-center px-1.5 md:px-2 py-0.5 rounded-full text-[10px] md:text-xs font-semibold border"
                  >
                    {{ trade.side === 'LONG' ? 'L' : 'S' }}
                  </span>
                </td>
       
                <!-- 开/平仓价（合并显示） -->
                <td class="hidden sm:table-cell px-2 md:px-4 py-2.5 md:py-3 text-right font-mono tabular-nums whitespace-nowrap">
                  <div class="text-slate-300 text-[11px] md:text-xs">{{ formatPriceWithPrecision(trade.avgOpenPrice, trade.symbol, trade.exchange) }}</div>
                  <div class="text-slate-500 text-[10px]">{{ formatPriceWithPrecision(trade.avgClosePrice, trade.symbol, trade.exchange) }}</div>
                </td>
            
                <!-- 净收益 + 峰值收益（合并显示） -->
                <td class="px-2 md:px-4 py-2.5 md:py-3 text-right font-mono tabular-nums whitespace-nowrap">
                  <!-- 净收益（主） -->
                  <div
                    :class="Number(trade.netPnl || 0) >= 0 ? 'text-green-400' : 'text-red-400'"
                    class="font-semibold text-[11px] md:text-sm"
                  >
                    {{ Number(trade.netPnl || 0) >= 0 ? '+' : '' }}{{ Number(trade.netPnl).toFixed(2) }}
                    <span v-if="calcPct(trade.netPnl, trade) !== null" class="text-[9px] md:text-[10px] opacity-80">
                      ({{ calcPct(trade.netPnl, trade) >= 0 ? '+' : '' }}{{ calcPct(trade.netPnl, trade).toFixed(1) }}%)
                    </span>
                  </div>
                  <!-- 峰值收益（副） -->
                  <div class="text-[10px] text-slate-500 mt-0.5" :title="t('dashboard.peakPnl')">
                    <span class="text-green-400/60">↑{{ Number(trade.peakPnl || 0).toFixed(2) }}</span>
                  </div>
                </td>
            
                <!-- 峰值回撤 -->
                <td class="hidden md:table-cell px-2 md:px-4 py-2.5 md:py-3 text-right font-mono tabular-nums whitespace-nowrap">
                  <div class="text-red-400 text-[11px] md:text-xs">
                    {{ Number(trade.drawdownToClose || 0) > 0 ? '-' + Number(trade.drawdownToClose).toFixed(2) : '0.00' }}
                  </div>
                  <div v-if="calcDdFromPeakPct(trade) !== null && Number(trade.drawdownToClose || 0) > 0"
                       class="text-[10px] text-red-400/60">
                    -{{ calcDdFromPeakPct(trade).toFixed(1) }}%
                  </div>
                </td>
            
                <!-- 持仓时长 -->
                <td class="hidden lg:table-cell px-2 md:px-4 py-2.5 md:py-3 text-center text-slate-300 whitespace-nowrap font-mono">
                  {{ formatDuration(trade.durationMs) }}
                </td>
            
                <!-- 手续费 -->
                <td class="hidden xl:table-cell px-2 md:px-4 py-2.5 md:py-3 text-right text-slate-300 font-mono tabular-nums whitespace-nowrap">
                  {{ Number(trade.feeTotal || 0).toFixed(2) }}
                </td>
            
                <!-- 开平时间（合并显示） -->
                <td class="hidden sm:table-cell px-2 md:px-4 py-2.5 md:py-3 text-right text-slate-400 text-[10px] whitespace-nowrap">
                  <div>{{ formatTime(trade.openTimeMs) }}</div>
                  <div class="text-slate-500">{{ formatTime(trade.closeTimeMs) }}</div>
                </td>
              </tr>
            </tbody>
          </table>
        </div>

        <div v-if="closedTrades.length === 0" class="text-center py-12 text-slate-400">
          {{ t('dashboard.noClosedTrades') }}
        </div>
      </div>
    </div>
  </div>
</div>
  `,
}).mount("#app");