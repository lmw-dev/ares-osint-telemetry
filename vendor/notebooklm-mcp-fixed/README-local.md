## Local NotebookLM MCP Fix

This directory vendors a local fixed copy of `notebooklm-mcp` and its
dependencies so Codex does not depend on `npx notebooklm-mcp@latest` or the
mutable npm cache.

Base package:

- `notebooklm-mcp@2.0.0`

Why this local fork exists:

- The upstream `add_source` flow was failing against the current Chinese
  NotebookLM UI.
- The original selector logic matched hidden non-source dialogs such as the
  emoji keyboard overlay.
- The original overlay input selectors could resolve the dialog container
  instead of the actual editable field.
- The current NotebookLM website source flow uses a Chinese `插入` button with
  `mdc-button--unelevated`, which upstream `insertConfirm` selectors did not
  cover.

Local selector fixes applied in:

- `node_modules/notebooklm-mcp/dist/notebooklm/selectors.js`

Applied changes:

- Exclude `.emoji-keyboard__container` from source dialog matching.
- Narrow overlay input/textarea selectors to actual editable descendants.
- Add Chinese submit button support:
  - `button.mdc-button--unelevated:has-text("插入")`
  - `button:has-text("插入")`
  - `button[aria-label="提交"]`

Validation:

- Direct black-box regression against NotebookLM succeeded.
- `add_source` completed with source count increase from `5 -> 6`.

Recommended Codex config target:

- `command = "node"`
- `args = ["/Users/liumingwei/01-project/12-liumw/21-ares-osint-telemetry/vendor/notebooklm-mcp-fixed/node_modules/notebooklm-mcp/dist/index.js"]`
