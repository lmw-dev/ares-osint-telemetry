# Ares OSINT Telemetry - 赛前赔率快照与阵地交接文档

## 模块定位：冷热数据转换的起搏器

本次工程迭代（2026.04）重点重构了 `osint_crawler.py`。该模块作为 Ares 量化引擎“吸收情报”的最前沿兵种，现已具备“两开花”的绝对统治力：
1. **破冰对接（身份打通）**：将中国体彩（C端）赛程与 Understat 数据库（B端）战术 ID 强力铆接。
2. **盘口汲取（金融抽血）**：零成本掠夺博彩机构的初始水位（欧赔、亚盘、凯利指数、预测概率）。

---

## 核心架构设计与决策分析

### 1. 为什么“抓变化”采用历史栈（History Stack）模式？
**需求本质**：量化引擎需要洞悉主力的资金流向变动（走地/降调）。
**架构考量**：如果采用传统的关系型数据库需要起服务。我们选择了极其轻量的 `Snapshot Appending`（快照追加）模式。
通过赋予 `[issue]_dispatch_manifest.json` **时序成长性**，每次爬虫启动时：
- 它会**反省本地**是否存在老订单。
- 存在，即刻转入 Tracking 模式，跳过极其耗时的 Understat 对接（防封锁）。
- 将刚刚抓到的水位挂载当前国际标准时间戳（`timestamp: "2026-04-19T..."Z`），压入 `market_odds_history` 数组栈内。

这样不仅最大程度节约了网络开销，更为后期计算波段方差（Variance Engine）铺平了大道。

### 2. 解析器选型（HTML DOM vs Headless）
我们继续保持了“极简极速”的理念，采用原生 Python Regex 的降维手段。
直接扒取源码中 `<tr data-asian="0.96,半球,0.88">` 这样的结构，并自动实施容错分割（`try ... except` 防崩），使得运行速度控制在毫秒级，规避了大量因数据渲染拖延导致的 Timeout 隐患。

---

## 下一步架构推进建议 (Deploy / Ops Suggestions)

### 📈 定时嗅探器 (Cron Agent)
既然脚本已具备**防抖追踪与快照成长功能**，为了真正抓取“变化”，请立刻在服务器部署自动化守护进程。
*配置示例* (针对周六/日赛事密集期的高频心跳)：
```cron
# 每两小时执行一次赛前心跳嗅探
0 */2 * * 6,0 cd /path/ares-osint-telemetry && \
source venv/bin/activate && \
python src/data/osint_crawler.py --issue 26063
```
*每一次心跳，该数组中就会长出一个新的时间序列点位。等到比赛吹哨，您的量化模型将会拥有一条完整的主力诱盘曲线！*

### 📊 报警网关 (Alert Routing)
后续可以在 `scan_and_map` 的动态追踪模块旁，埋入**异动钩子（Hook）**：
当 `new_asian_handicap['home']` 与上一份历史记录中的 `home` 水位差值 `delta > 0.1` 时，直接触发飞书/Telegram Bot 的警报，提醒您有大额聪明筹码入场。

Ares Crawler 节点改装完毕，向超级量化终端（Ares v4.1）交付！
