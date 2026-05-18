# YouTube 自动化监控模块集成总结

依据 Issue **TOM-767** 的要求，已将 YouTube 视频监控功能集成至 `21-ares-osint-telemetry` 项目。

## 1. 核心改动记录

### 1.1 新增模块
- **[youtube_monitor.py](file:///Users/liumingwei/01-project/12-liumw/21-ares-osint-telemetry/src/data/youtube_monitor.py)**: 核心执行脚本。
  - **混合驱动**：支持 YouTube Data API (需 Key) 和 `yt-dlp` (免 Key 抓取)。
  - **自动化过滤**：默认仅收集时长在 10-25 分钟、且在过去 14 天内发布的视频。
  - **去重逻辑**：通过 `seen_video_ids.txt` 和 CSV 预检双重保证。
- **[youtube_channels.yaml](file:///Users/liumingwei/01-project/12-liumw/21-ares-osint-telemetry/config/youtube_channels.yaml)**: 频道配置文件，默认包含 Alex Finn 和 Riley Brown。
- **[com.lmw.youtube-monitor.plist](file:///Users/liumingwei/01-project/12-liumw/21-ares-osint-telemetry/scripts/launchd/com.lmw.youtube-monitor.plist)**: macOS 定时任务模板。

### 1.2 环境与配置
- **requirements.txt**: 新增 `yt-dlp`、`google-api-python-client` 和 `python-dotenv`。
- **.env / .env.example**: 新增 `YOUTUBE_API_KEY` 字段。
- **.gitignore**: 已将 `data/youtube_monitor/` 下的产出物排除在版本控制之外。

## 2. 使用指南

### 2.1 手动运行
```bash
# 激活虚拟环境
source venv/bin/activate
# 执行监控
python src/data/youtube_monitor.py
```

### 2.2 常用参数
- `--recent-days`: 收集最近几天的视频（默认 14）。
- `--min-minutes`: 最小分钟数（默认 10）。
- `--max-minutes`: 最大分钟数（默认 25）。

### 2.3 部署定时任务
1. 编辑 `scripts/launchd/com.lmw.youtube-monitor.plist`，确保 Python 和工作目录路径正确。
2. 运行以下命令加载任务：
   ```bash
   cp scripts/launchd/com.lmw.youtube-monitor.plist ~/Library/LaunchAgents/
   launchctl load ~/Library/LaunchAgents/com.lmw.youtube-monitor.plist
   ```

## 3. 后续建议
- **数据回填**：后续可编写脚本将 `videos.csv` 中的内容自动同步至 Ares Vault 的球队情报库。
- **代理支持**：若在某些环境下访问 YouTube 困难，可在 `YouTubeMonitor._fetch_via_yt_dlp` 中增加代理配置。

---
**Agent**: Antigravity
**Date**: 2026-05-14
