---
name: youtube-transcript-ingestion
version: "1.0"
source: ares-osint-telemetry native
description: >
  YouTube 字幕/转录文本提取 Skill（v1.0）。
  通过 yt-dlp 字幕专项提取，将 YouTube 视频字幕保存为 AresVault 原始 RAG 素材。
  本 Skill 只负责获取原始 transcript 文本，不做 claim extraction，不做 validation，不做 Team Archive patch。
  对应工作流节点：YT-02（transcript-first 主路径）。
inputs:
  required:
    - source_url: YouTube 视频 URL
    - target_team: 目标球队名称（英文标准名）
    - source_channel: 来源频道名称
    - source_date: 视频发布日期（YYYY-MM-DD）或输出日期
  optional:
    - video_id: YouTube video_id（不提供时从 source_url 自动解析）
    - target_league: 目标联赛（EPL / La_liga / Serie_A / Bundesliga / Ligue_1）
    - coach_context: 相关教练（如 Arteta）
    - language_preference: 字幕语言偏好（en / zh / auto，默认 auto）
    - output_basename: 自定义输出文件名（不含扩展名）
    - notes: 备注
outputs:
  - Raw Transcript（Markdown）→
      /Users/liumingwei/vaults/AresVault/04_RAG_Raw_Data/youtube_tactical_sources/transcripts/
      <date>_<team>_<channel>_<video_id>_transcript_raw.md
  - Blocked Report（Markdown，仅提取失败时）→
      /Users/liumingwei/vaults/AresVault/04_RAG_Raw_Data/youtube_tactical_sources/transcripts/blocked/
      <date>_<team>_<channel>_<video_id>_transcript_blocked.md
changelog:
  - v1.0 (2026-05-27): initial release，基于 LMW-118 Arsenal/Tifo pilot 验证的 yt-dlp 主路径
---

# YouTube Transcript Ingestion Skill v1.0

## 执行模式说明

本 Skill 是一套**模型无关的 Agent 执行规范**。当你在 Antigravity / Kiro 中被用户通过 `@skill` 或文件路径方式调用时，请按照本文件的步骤逐一执行。

**你是 transcript 提取引擎，不是内容分析引擎。** 你的职责是：
1. 接收 YouTube URL 和必要元数据
2. 通过 `yt-dlp` 提取字幕（仅字幕，不下载视频/音频）
3. 将原始 transcript 文本写入 AresVault 指定目录
4. 提取失败时生成 blocked report，不猜测内容

**严格边界**：
- ✅ 提取 YouTube 字幕/转录文本
- ✅ 解析 video_id（当未提供时）
- ✅ 生成带 frontmatter 的原始 transcript markdown
- ✅ 提取失败时生成 blocked report
- ❌ 不下载视频
- ❌ 不下载音频
- ❌ 不做 claim extraction
- ❌ 不做 claim validation
- ❌ 不修改 Team Archive
- ❌ 不写入 claims/、validation/、notebooklm_outputs/
- ❌ 不生成 prematch 结论
- ❌ 提取失败时不猜测 transcript 内容

---

## AresVault 路径模型

```text
Vault Base: /Users/liumingwei/vaults/AresVault

1. Skill 代码（代码项目）
   src/skills/youtube-transcript-ingestion/SKILL.md

2. 治理规则 / 模板（AresVault 01_Governance）
   01_Governance/规范 - Ares YouTube 原文提取与字幕摄取规则 v1.md
   01_Governance/模板 - YouTube transcript raw output v1.md
   01_Governance/模板 - YouTube transcript blocked report v1.md

3. 原始 transcript 输出
   04_RAG_Raw_Data/youtube_tactical_sources/transcripts/
     <date>_<team>_<channel>_<video_id>_transcript_raw.md

4. Blocked report 输出
   04_RAG_Raw_Data/youtube_tactical_sources/transcripts/blocked/
     <date>_<team>_<channel>_<video_id>_transcript_blocked.md
```

---

## 核心原则

1. **Transcript-first**。本 Skill 是 YT-02 主路径，不依赖 NotebookLM。
2. **只取原文，不做解读**。输出是原始字幕文本，不包含任何战术判断。
3. **yt-dlp 字幕专项**。必须使用 `--skip-download`，严禁下载视频/音频。
4. **browser cookies 显式声明**。使用 Chrome cookies 时必须在 frontmatter 中记录。
5. **失败即 blocked report**。提取失败时生成 blocked report，不猜测内容。
6. **Truth > Completeness**。宁可输出 blocked report，不输出不确定的内容。

---

## 提取优先级（Extraction Priority）

```
主路径（必须优先尝试）：
  1. yt-dlp 字幕专项（--skip-download + browser cookies when configured）

可选 fallback（仅在主路径失败且明确配置时）：
  2. YouTube Transcript MCP（仅当已安装且明确配置时）
  3. youtube-transcript-api（仅当不需要下载视频/音频时）

手动 fallback（最后手段）：
  4. 生成 blocked report，提示用户手动处理

NotebookLM 不是本 Skill 的提取路径。
```

---

## 执行流程（4 Phases）

### Phase 1: 参数解析与路径准备

1. 从用户输入中提取必填字段：`source_url`、`target_team`、`source_channel`、`source_date`
2. 若未提供 `video_id`，从 `source_url` 解析：
   ```python
   # 支持格式：
   # https://www.youtube.com/watch?v=GxvSAS97L9c  → GxvSAS97L9c
   # https://youtu.be/GxvSAS97L9c                 → GxvSAS97L9c
   import re
   match = re.search(r'(?:v=|youtu\.be/)([A-Za-z0-9_-]{11})', source_url)
   video_id = match.group(1) if match else "UNKNOWN"
   ```
3. 构建输出文件名：
   ```
   <source_date>_<target_team>_<source_channel>_<video_id>_transcript_raw.md
   示例：2026-05-22_Arsenal_Tifo_GxvSAS97L9c_transcript_raw.md
   ```
4. 确认输出目录存在：
   ```
   /Users/liumingwei/vaults/AresVault/04_RAG_Raw_Data/youtube_tactical_sources/transcripts/
   /Users/liumingwei/vaults/AresVault/04_RAG_Raw_Data/youtube_tactical_sources/transcripts/blocked/
   ```

### Phase 2: yt-dlp 字幕提取

**标准命令（推荐，使用 Chrome cookies）**：
```bash
yt-dlp \
  --skip-download \
  --write-subs \
  --write-auto-subs \
  --sub-langs "en,zh-Hans,zh-Hant" \
  --sub-format "vtt/srt/best" \
  --cookies-from-browser chrome \
  --output "/tmp/ares_transcript_%(id)s.%(ext)s" \
  "https://www.youtube.com/watch?v=GxvSAS97L9c"
```

**不使用 cookies 的命令（当 cookies 不可用时）**：
```bash
yt-dlp \
  --skip-download \
  --write-subs \
  --write-auto-subs \
  --sub-langs "en,zh-Hans,zh-Hant" \
  --sub-format "vtt/srt/best" \
  --output "/tmp/ares_transcript_%(id)s.%(ext)s" \
  "https://www.youtube.com/watch?v=GxvSAS97L9c"
```

**⚠️ 必须遵守的 yt-dlp 规则**：
- 必须包含 `--skip-download`
- 不得使用 `-x`（音频提取）
- 不得使用 `--format`（视频格式选择）
- 不得下载任何媒体文件
- 使用 browser cookies 时必须在 frontmatter 中记录 `used_browser_cookies: true`

**字幕文件处理**：
1. yt-dlp 输出 `.vtt` 或 `.srt` 文件到 `/tmp/`
2. 读取字幕文件内容
3. 清洗时间戳格式（保留时间戳，但统一格式）
4. 写入 AresVault transcript markdown

### Phase 3: 输出文件生成

**成功时**：生成 `_transcript_raw.md`（见下方格式规范）

**失败时**：生成 `_transcript_blocked.md`（见下方 blocked report 规范）

触发 blocked report 的条件：
- 无可用字幕（视频无字幕且无自动字幕）
- browser cookie 认证失败
- YouTube 访问受限（地区限制、年龄限制等）
- yt-dlp 执行失败（任何非零退出码）
- 字幕文件为空
- 字幕内容过于嘈杂（如纯音乐视频的自动字幕）
- 必要元数据缺失且无法安全推断

### Phase 4: 验证与报告

1. 确认输出文件已写入正确路径
2. 验证 frontmatter 完整性
3. 向用户报告：
   - 输出文件路径
   - 使用的提取方法
   - 是否使用了 browser cookies
   - 字幕语言
   - 字幕行数/字数（估算）
   - 下一步建议（YT-03 claim extraction）

---

## 输出文件格式规范

### Raw Transcript Markdown（`_transcript_raw.md`）

```markdown
---
source_kind: youtube_video
video_id: GxvSAS97L9c
source_url: https://www.youtube.com/watch?v=GxvSAS97L9c
target_team: Arsenal
target_league: EPL
coach_context: Arteta
source_channel: Tifo Football
transcript_source: yt_dlp_subtitles
extraction_method: yt-dlp --skip-download --write-subs --cookies-from-browser chrome
language: en
include_timestamps: true
used_browser_cookies: true
source_authority: raw_transcript
profile_authority: false
created_at: 2026-05-22T10:30:00Z
downstream_allowed:
  - claim_extraction
  - validation
downstream_forbidden:
  - direct_team_archive_patch
  - claim_extraction_during_ingestion
  - video_download
  - audio_download
---

# Raw Transcript — Arsenal — Tifo Football — GxvSAS97L9c

## Source Metadata

| Field | Value |
|-------|-------|
| Video URL | https://www.youtube.com/watch?v=GxvSAS97L9c |
| Video ID | GxvSAS97L9c |
| Channel | Tifo Football |
| Target Team | Arsenal |
| Target League | EPL |
| Coach Context | Arteta |
| Published | 2026-05-22 |

## Extraction Metadata

| Field | Value |
|-------|-------|
| Transcript Source | yt_dlp_subtitles |
| Extraction Method | yt-dlp --skip-download --write-subs |
| Language | en |
| Timestamps Included | true |
| Browser Cookies Used | true |
| Extracted At | 2026-05-22T10:30:00Z |

## ⚠️ Boundary Notice

> **This file is raw transcript material only.**
>
> - ✅ Allowed as input for: claim extraction (YT-03), validation (YT-04)
> - ❌ Must NOT directly patch Team Archive
> - ❌ Must NOT be treated as verified tactical memory
> - ❌ Must NOT be used to generate prematch conclusions without YT-04 validation

---

## Raw Transcript

[00:00:00] Welcome to Tifo Football...
[00:00:15] Today we're looking at Arsenal's tactical setup under Arteta...
...

---

## Extraction Notes

- Subtitle type: auto-generated (YouTube ASR)
- Language detected: en
- Timestamp format: HH:MM:SS
- Total lines: ~450
- Estimated word count: ~3200

## Handoff Note

Next step: YT-03 Transcript-to-Tactical-Claims Extraction
Input path: 04_RAG_Raw_Data/youtube_tactical_sources/transcripts/<this_file>
```

---

## Blocked Report 格式规范

### Blocked Report Markdown（`_transcript_blocked.md`）

```markdown
---
source_kind: youtube_video
video_id: GxvSAS97L9c
source_url: https://www.youtube.com/watch?v=GxvSAS97L9c
target_team: Arsenal
source_channel: Tifo Football
blocked_at: 2026-05-22T10:30:00Z
attempted_method: yt-dlp --skip-download --write-subs --cookies-from-browser chrome
used_browser_cookies: true
failure_reason: no_subtitles_available
status: blocked
---

# Transcript Blocked Report — Arsenal — Tifo Football — GxvSAS97L9c

## Blocked Summary

| Field | Value |
|-------|-------|
| Video URL | https://www.youtube.com/watch?v=GxvSAS97L9c |
| Video ID | GxvSAS97L9c |
| Channel | Tifo Football |
| Target Team | Arsenal |
| Attempted Method | yt-dlp --skip-download --write-subs --cookies-from-browser chrome |
| Browser Cookies Used | true |
| Failure Reason | no_subtitles_available |
| Blocked At | 2026-05-22T10:30:00Z |

## Failure Details

yt-dlp 执行完成但未找到可用字幕。视频可能：
- 未开启字幕
- 仅有自动字幕但质量过低
- 字幕语言不在请求范围内

## Next Suggested Action

1. 在 YouTube 手动确认视频是否有字幕
2. 尝试不同语言：`--sub-langs "en.*,zh.*"`
3. 若确认无字幕，考虑 YT-02b NotebookLM secondary synthesis（optional fallback）
4. 或跳过此视频，寻找替代来源

## Note

> Transcript content was NOT guessed or fabricated.
> Truth > Completeness.
```

---

## 命令示例

### 基础用法（使用 Chrome cookies）

```bash
# LMW-118 Arsenal/Tifo 验证案例复现
yt-dlp \
  --skip-download \
  --write-subs \
  --write-auto-subs \
  --sub-langs "en" \
  --sub-format "vtt/srt/best" \
  --cookies-from-browser chrome \
  --output "/tmp/ares_transcript_%(id)s.%(ext)s" \
  "https://www.youtube.com/watch?v=GxvSAS97L9c"
```

### 不使用 cookies（公开视频）

```bash
yt-dlp \
  --skip-download \
  --write-subs \
  --write-auto-subs \
  --sub-langs "en,zh-Hans" \
  --sub-format "vtt/srt/best" \
  --output "/tmp/ares_transcript_%(id)s.%(ext)s" \
  "https://www.youtube.com/watch?v=VIDEO_ID"
```

### 检查可用字幕（不提取，仅列出）

```bash
yt-dlp \
  --skip-download \
  --list-subs \
  --cookies-from-browser chrome \
  "https://www.youtube.com/watch?v=VIDEO_ID"
```

### Helper Script（可选）

```bash
# 使用项目内 helper script（如已创建）
./venv/bin/python src/skills/youtube-transcript-ingestion/scripts/fetch_transcript.py \
  --url "https://www.youtube.com/watch?v=GxvSAS97L9c" \
  --team "Arsenal" \
  --channel "Tifo" \
  --date "2026-05-22" \
  --use-cookies
```

---

## Fallback 行为

```
主路径失败时的 fallback 顺序：

1. yt-dlp + Chrome cookies → 失败
   ↓
2. yt-dlp 不使用 cookies → 失败
   ↓
3. YouTube Transcript MCP（仅当已安装且明确配置）→ 失败
   ↓
4. youtube-transcript-api（仅当不需要下载媒体）→ 失败
   ↓
5. 生成 blocked report，提示用户手动处理

每一步失败时，记录失败原因到 blocked report。
不跳过 blocked report 直接进入下一步。
```

---

## NotebookLM 的定位

NotebookLM **不是**本 Skill 的提取路径。

- 本 Skill（YT-02）只负责 transcript 提取
- NotebookLM 是 YT-02b，optional secondary synthesis
- 仅在 transcript 不可获取时，才考虑 YT-02b
- NotebookLM 输出的 `source_authority` 永远是 `secondary_synthesis`，不升格为 `primary`

---

## 验证清单（v1.0）

交付前确认：
- [ ] `source_url` 已提供且格式正确
- [ ] `video_id` 已解析（提供或从 URL 自动解析）
- [ ] 使用了 `--skip-download`（无视频/音频下载）
- [ ] browser cookies 使用情况已在 frontmatter 中记录
- [ ] 输出文件路径符合命名规范
- [ ] frontmatter 包含所有必填字段
- [ ] `downstream_allowed` 和 `downstream_forbidden` 已声明
- [ ] 提取失败时生成了 blocked report（不猜测内容）
- [ ] 未做 claim extraction
- [ ] 未修改 Team Archive

---

## 打包资源

- `AresVault/01_Governance/规范 - Ares YouTube 原文提取与字幕摄取规则 v1.md` — 治理规范
- `AresVault/01_Governance/模板 - YouTube transcript raw output v1.md` — 输出模板
- `AresVault/01_Governance/模板 - YouTube transcript blocked report v1.md` — blocked report 模板
- `AresVault/01_Governance/规范 - Ares Transcript-first YouTube 战术摄取工作流 v1.md` — 工作流规范（LMW-120）
- `src/skills/youtube-transcript-ingestion/scripts/fetch_transcript.py` — 可选 helper script
