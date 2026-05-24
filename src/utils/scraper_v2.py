import asyncio
import logging
from pathlib import Path
from typing import Optional
from playwright.async_api import async_playwright

logger = logging.getLogger("AresTelemetry.ScraperV2")

async def fetch_html_via_playwright_v2(
    url: str,
    headless: bool = True,
    state_file: Optional[str] = "tmp/browser_state.json",
    timeout_ms: int = 30000,
    wait_until: str = "domcontentloaded",
    wait_for_selector: Optional[str] = None
) -> str:
    """
    [Ares Telemetry Scraper V2]
    原子级高隐蔽性 Playwright 网页抓取函数。
    
    特点：
    1. 极简调用：像 requests 一样单行调用，完全隐藏浏览器启动与生命周期管理。
    2. Stealth 隐身：强力抹除 WebDriver 自动化特征，注入真人行为指纹。
    3. 会话继承：支持在指定路径（如 tmp/browser_state.json）持久化 Cookies，防止高频验证。
    4. 防挂死：全链路 try...finally 机制，确保无论成功与否，物理浏览器必定 100% 优雅关闭。
    """
    playwright_instance = None
    browser = None
    context = None
    
    try:
        # 1. 初始化 Playwright 实例
        playwright_instance = await async_playwright().start()
        
        # 2. 隐密拉起 Chromium 内核
        browser = await playwright_instance.chromium.launch(
            headless=headless,
            args=[
                "--disable-blink-features=AutomationControlled", # 强力抹除自动化标志
                "--no-sandbox",
                "--disable-gpu"
            ]
        )
        
        # 3. 会话状态处理
        context_kwargs = {
            "viewport": {"width": 1280, "height": 720},
            "user_agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
            )
        }
        
        if state_file:
            state_path = Path(state_file)
            state_path.parent.mkdir(parents=True, exist_ok=True)
            if state_path.exists():
                context = await browser.new_context(storage_state=str(state_path), **context_kwargs)
                logger.info(f"[ScraperV2] 继承本地 Cookies: {state_file}")
            else:
                context = await browser.new_context(**context_kwargs)
        else:
            context = await browser.new_context(**context_kwargs)
 
        # 4. 新建 Page 并在页面加载前强力注入 Stealth 隐身指纹
        page = await context.new_page()
        await page.add_init_script("""
            // 擦除 window.navigator.webdriver 痕迹
            Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
            // 模拟真人硬件指纹
            window.chrome = { runtime: {} };
            Object.defineProperty(navigator, 'languages', { get: () => ['zh-CN', 'zh'] });
        """)
 
        # 5. 执行网页拉取
        logger.info(f"[ScraperV2] 开始执行高保真渲染抓取 (wait_until={wait_until}): {url}")
        await page.goto(url, wait_until=wait_until, timeout=timeout_ms)
        
        if wait_for_selector:
            logger.info(f"[ScraperV2] 等待特定 DOM 元素出现: {wait_for_selector}")
            try:
                await page.wait_for_selector(wait_for_selector, timeout=10000)
            except Exception as e:
                logger.warning(f"[ScraperV2] 等待特定元素超时: {e}，将直接读取当前内容。")
        
        # 6. 如果遇到 Cloudflare 盾或安全验证，给出高亮提示（有头模式下可用）
        content = await page.content()
        if "checking your browser" in content or "cloudflare" in page.url:
            if headless:
                # 提示外部调度器升级为有头模式重试
                raise PermissionError("[ScraperV2] 遭遇人机防御阻断，无头模式受限，需要有头辅助授权。")
            else:
                logger.warning("[ScraperV2] ⚠️ 遭遇强力安全挑战，请在浏览器中手动完成滑块人机验证...")
                # 等待 60 秒供人工拖动
                for _ in range(30):
                    await asyncio.sleep(2)
                    content = await page.content()
                    if "checking your browser" not in content:
                        logger.info("[ScraperV2] 🎉 手动验证通过，正在回写 Cookies 以便后台长效使用...")
                        if state_file:
                            await context.storage_state(path=str(state_file))
                        break
                else:
                    raise TimeoutError("[ScraperV2] 人机挑战验证超时！")

        # 7. 捕获完整动态渲染后的网页并返回
        return content

    except Exception as e:
        logger.error(f"[ScraperV2] 抓取失败，引发异常: {e}")
        raise e
        
    finally:
        # 8. 终极物理释放（物理关机，拒绝僵尸浏览器进程）
        if context:
            await context.close()
        if browser:
            await browser.close()
        if playwright_instance:
            await playwright_instance.stop()
        logger.info("[ScraperV2] 物理资源与浏览器进程已 100% 优雅释放")
