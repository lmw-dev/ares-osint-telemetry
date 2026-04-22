# Ares 跨网映射引擎技术文档 (Crawler Internal API Bypass)

> **"数据不是被封锁的，它只是被混淆了。"**

本文档记录了 Ares OSINT Telemetry Pipeline 中 `osint_crawler.py` 的核心破壁技术实现与演进。该引擎负责将中国足彩的简单期号（如 `26063`）与海外 Understat xG 数据库的真实比赛 ID 智能接轨。

## 背景：前端防守与反爬困局

在初期工程中，我们遭遇了严密的防爬火力网：
1. **Understat XHR 改造**：Understat 不再于 HTML `window.datesData` 中直出赛历数据，导致传统的 `requests.get` + 正则切分完全失效。
2. **DuckDuckGo OSINT 熔断**：尝试利用搜索引擎（反向搜索）时，连续高频的并发探测瞬间触发了搜索引擎的防机刷 (anomaly.js) 中枢封锁。
3. **Selenium 负担**：引入 Headless Browser 可破局，但会使 Ares 轻量级节点的依赖雪崩（需携带数GB Chromium）。

## 核心实现：XHR 隐密接口逆向

我们通过对 Understat 前端 `league.min.js` 进行逆向分析，剥离出了其内部渲染数据的隐藏 API。
关键破解逻辑在于**头部伪装 (Header Spoofing)**。

### Python 裸请求代码：
```python
url = f"https://understat.com/getLeagueData/EPL/2023"
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    # 【致胜关键】通知服端此为底层 ajax 数据交互，逼迫其返回未加工的源 JSON
    "X-Requested-With": "XMLHttpRequest" 
}
resp = requests.get(url, headers=headers)
data = resp.json()
# data['dates'] -> 全年 380 场比赛一览无余
```

## 技术优势

1. **绝对原生 (Zero-Dependency)**：没有 Selenium/Playwright 包袱，容器级部署秒启。
2. **免商业授权 (Zero-Cost)**：避免了每年千元以上的第三方诸如 API-Football 或 Sportmonks 的数据订阅费用。
3. **防封锁极速映射 (Speed)**：获取五大联赛整年记录的请求仅需一次，内存驻留缓存。解析期号 14 场映射过程通常耗时 **< 2 秒**。

## 映射流程图 (The Pipeline)

1. **A端获取 (A-Node)**：正则提取 `trade.500.com/sfc/`，无损切割中国体彩 14 场对阵。
2. **B端建立 (B-Node)**：利用 `getLeagueData` 内部 API 打包构建最新连续两年的海外球队对象集群。
3. **C端融合 (C-Node)**：依赖 `team_alias_map.json` （支持缩写如“莱比锡”、“勒沃”）进行双端桥接，最终锁死对应 ID 落盘。
