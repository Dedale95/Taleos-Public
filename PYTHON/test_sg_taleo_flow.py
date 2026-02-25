#!/usr/bin/env python3
"""
Test du flux Taleo Société Générale (disclaimer, formulaire)
Usage: SG_USER=thibault.parisien SG_PASS='...' python test_sg_taleo_flow.py
"""
import asyncio
import os
from playwright.async_api import async_playwright

SG_USER = os.environ.get('SG_USER', '')
SG_PASS = os.environ.get('SG_PASS', '')
OFFER_URL = "https://careers.societegenerale.com/en/job-offers/junior-sales-manager-cash-management-m-f-d-25000DUF-en"

async def main():
    if not SG_USER or not SG_PASS:
        print("Usage: SG_USER=xxx SG_PASS='yyy' python test_sg_taleo_flow.py")
        return
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)
        context = await browser.new_context(viewport={'width': 1280, 'height': 800})
        page = await context.new_page()
        try:
            print("Navigation vers l'offre...")
            await page.goto(OFFER_URL, wait_until="domcontentloaded")
            await asyncio.sleep(3)
            # Fermer le bandeau cookies Didomi (shadow DOM possible)
            try:
                await page.evaluate("""() => {
                    const host = document.querySelector('#didomi-host');
                    const btn = host?.shadowRoot?.querySelector('#didomi-notice-disagree-button')
                        || document.querySelector('#didomi-notice-disagree-button');
                    if (btn) btn.click();
                    document.body.style.setProperty('overflow', 'auto', 'important');
                }""")
                print("   Cookies Didomi fermés.")
                await asyncio.sleep(2)
            except Exception:
                pass
            apply_btn = page.locator('a[data-gtm-label="postuler"]').first
            await apply_btn.wait_for(state="visible", timeout=15000)
            print("Clic Apply...")
            try:
                async with page.expect_navigation(timeout=15000):
                    await apply_btn.click(force=True)
            except Exception as nav_err:
                print("   Navigation:", nav_err)
            await asyncio.sleep(5)
            target = page
        except Exception as e:
            print("Apply:", e)
            target = page
        print("Page Taleo:", target.url)
        try:
            login_name = target.locator('#dialogTemplate-dialogForm-login-name1').first
            await login_name.wait_for(state="visible", timeout=20000)
            await login_name.fill(SG_USER)
            await target.fill('#dialogTemplate-dialogForm-login-password', SG_PASS)
            await target.click('#dialogTemplate-dialogForm-login-defaultCmd')
            print("Login envoyé, attente 10s...")
            await asyncio.sleep(10)
        except Exception as e:
            print("Login:", e)
        html = await target.content()
        out = os.path.join(os.path.dirname(__file__), "sg_after_login.html")
        with open(out, "w", encoding="utf-8") as f:
            f.write(html)
        print("HTML sauvegardé dans", out)
        for attempt in range(6):
            clicked = False
            for sel in ['input[id*="legalDisclaimerContinueButton"]', 'input[id*="saveContinueCmdBottom"]', 'input[value*="Continue"]', 'input[value*="Save and Continue"]']:
                try:
                    el = target.locator(sel).first
                    if await el.count() > 0 and await el.is_visible():
                        print(f"Clic disclaimer {attempt + 1}:", sel)
                        await el.click()
                        await asyncio.sleep(5)
                        clicked = True
                        break
                except Exception:
                    pass
            if not clicked:
                await asyncio.sleep(2)
        print("Attente 20s pour inspection manuelle...")
        await asyncio.sleep(20)
        await browser.close()

if __name__ == "__main__":
    asyncio.run(main())
