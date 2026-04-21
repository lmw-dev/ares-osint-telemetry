# team_forge v4.2 升级记录（2026-04-21）

## 1. 目标
- 新增球队档案初始化脚本 `src/data/team_forge.py`。
- 保证输出 Frontmatter 对齐 Ares v4.2 三段结构。
- 强制落盘到 Vault 团队档案目录：`{ARES_VAULT_PATH}/02_Team_Archives/{league}/{team}.md`。

## 2. 实现内容
- 命令参数：
  - `--team`：队名（必填）
  - `--league`：联赛名（必填）
- 环境加载：
  - 自动读取项目根目录 `.env`（仅补充未设置环境变量，不覆盖 shell 现有值）
  - 校验 `ARES_VAULT_PATH`，缺失即报错退出
- 路径规则：
  - 自动创建 `02_Team_Archives/{league}` 目录
  - 文件名采用 `{team}.md`
  - 对 `team/league` 做非法路径字符清洗，规避路径穿越和非法文件名
- Frontmatter（v4.2）：
  - `intel_base`
  - `physical_reality`
  - `reality_gap`
- Markdown 正文扩展性：
  - 若目标文件已存在，保留其正文，仅替换/升级 frontmatter
  - 若不存在，写入默认可扩展正文模板
- 写入方式：
  - 使用 `yaml.safe_dump` 构建 frontmatter
  - 采用临时文件替换方式完成安全落盘

## 3. 验证
- `./venv/bin/python -m py_compile src/data/team_forge.py`
- `ARES_VAULT_PATH=/tmp/ares_vault_test ./venv/bin/python src/data/team_forge.py --team Arsenal --league EPL`
