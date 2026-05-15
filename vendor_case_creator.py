"""
Vendor Central Case Creator
============================
Automatically submits one case per ASIN to remove it from variations.

Instructions:
  1. Edit ASINS list below (or point ASIN_FILE to a .txt file, one ASIN per line)
  2. Run: python vendor_case_creator.py
  3. Log into Vendor Central in the browser window that opens
  4. The script waits until you're fully logged in, then takes over automatically
"""

import csv
import time
from datetime import datetime
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError

CONTACT_URL = "https://vendorcentral.amazon.com/hz/vendor/members/contact"

# ── ASIN list ─────────────────────────────────────────────────────────────────
# Option A: put ASINs directly here
ASINS = [
    "B08L8NZ765",
    "B096GCMH11",
]

# Option B: load from a text file (one ASIN per line) — set path or leave ""
ASIN_FILE = "C:/Users/makep/Documents/Amazon-Stuff/asins_to_remove.txt"

# ── Submission log ─────────────────────────────────────────────────────────────
LOG_FILE = "C:/Users/makep/Documents/Amazon-Stuff/submitted_cases_log.csv"

DELAY_BETWEEN_CASES = 4   # seconds to wait between submissions
CATEGORY_LABEL = "Product Linking (Variations, Twister, Newer Version, Duplicates, Edition, Titleset)"


def load_asins():
    asins = list(ASINS)
    if ASIN_FILE:
        try:
            with open(ASIN_FILE, encoding="utf-8") as f:
                for line in f:
                    asin = line.strip()
                    if asin and not asin.startswith("#"):
                        asins.append(asin)
        except FileNotFoundError:
            pass
    return [a for a in asins if a]


def build_message(asin):
    return (
        f"Hi Amazon Team,\n\n"
        f"Please help remove\n"
        f"{asin}\n"
        f"From variations\n\n"
        f"Thank you"
    )


def wait_for_login(page):
    """Wait until the page is no longer on a signin URL."""
    print("  Waiting for login to complete...")
    for _ in range(120):   # wait up to 2 minutes
        if "signin" not in page.url and "ap/signin" not in page.url:
            print("  Logged in.")
            return True
        time.sleep(1)
    print("  Timed out waiting for login.")
    return False


def navigate_to_contact(page):
    """Navigate to contact page, handling any signin redirects."""
    try:
        page.goto(CONTACT_URL, wait_until="load", timeout=60000)
    except Exception:
        # Might be interrupted by a redirect — check where we are
        pass

    # If redirected to signin, wait for user to log in
    if "signin" in page.url or "ap/signin" in page.url:
        print("  Redirected to signin — please log in in the browser...")
        if not wait_for_login(page):
            return False
        # Navigate again after login
        try:
            page.goto(CONTACT_URL, wait_until="load", timeout=60000)
        except Exception:
            pass

    page.wait_for_timeout(2000)
    return True


def submit_case(page, asin):
    print(f"  [{asin}] Navigating to contact page...")
    if not navigate_to_contact(page):
        return False

    # Step 1: Click "Manage My Catalog" to expand it
    print(f"  [{asin}] Clicking 'Manage My Catalog'...")
    try:
        manage_catalog = page.get_by_text("Manage My Catalog", exact=True)
        manage_catalog.wait_for(timeout=10000)
        manage_catalog.click()
        page.wait_for_timeout(2000)
    except Exception as e:
        print(f"  [{asin}] ERROR finding 'Manage My Catalog': {e}")
        return False

    # Step 2: Scroll down and click "Product Linking..." link
    print(f"  [{asin}] Scrolling to and clicking 'Product Linking...'...")
    try:
        product_linking = page.get_by_text("Product Linking", exact=False)
        product_linking.wait_for(timeout=8000)
        product_linking.scroll_into_view_if_needed()
        page.wait_for_timeout(800)
        product_linking.click()
        page.wait_for_timeout(2000)
        print(f"  [{asin}] 'Product Linking' clicked.")
    except Exception as e:
        print(f"  [{asin}] ERROR clicking 'Product Linking': {e}")
        return False

    # Step 3: Click "Still need help" button
    print(f"  [{asin}] Clicking 'Still need help'...")
    try:
        still_need_help = page.get_by_text("Still need help", exact=False)
        still_need_help.wait_for(timeout=8000)
        still_need_help.scroll_into_view_if_needed()
        page.wait_for_timeout(800)
        still_need_help.click()
        page.wait_for_timeout(2000)
        print(f"  [{asin}] 'Still need help' clicked.")
    except Exception as e:
        print(f"  [{asin}] ERROR finding 'Still need help': {e}")
        return False

    # Step 4: Use pyautogui to click the textarea and type (bypasses shadow DOM)
    print(f"  [{asin}] Typing message via OS keyboard...")
    page.wait_for_timeout(1000)
    try:
        import pyautogui
        import pyperclip

        # Get screen coordinates of kat-textarea via Playwright bounding box
        host = page.locator("kat-textarea").first
        host.wait_for(state="attached", timeout=8000)
        host.scroll_into_view_if_needed()
        page.wait_for_timeout(600)
        box = host.bounding_box()
        if not box:
            print(f"  [{asin}] ERROR: could not get textarea position")
            return False

        # Click center of the textarea using real OS mouse
        cx = box['x'] + box['width'] / 2
        cy = box['y'] + box['height'] / 2
        pyautogui.click(cx, cy)
        time.sleep(0.5)
        pyautogui.hotkey('ctrl', 'a')
        time.sleep(0.2)

        # Paste via clipboard (faster and handles special chars)
        pyperclip.copy(build_message(asin))
        pyautogui.hotkey('ctrl', 'v')
        time.sleep(0.5)
        print(f"  [{asin}] Message typed.")
    except Exception as e:
        print(f"  [{asin}] ERROR typing message: {e}")
        return False

    # Step 5: Click Email tab after filling textarea
    print(f"  [{asin}] Selecting Email tab...")
    try:
        email_tab = page.get_by_role("tab", name="Email")
        email_tab.wait_for(timeout=8000)
        email_tab.scroll_into_view_if_needed()
        page.wait_for_timeout(500)
        email_tab.click()
        page.wait_for_timeout(1500)
        print(f"  [{asin}] Email tab selected.")
    except Exception as e:
        print(f"  [{asin}] WARNING: Could not click Email tab: {e}")

    # Step 6: Click Send
    print(f"  [{asin}] Sending...")
    for submit_text in ["Send", "Submit", "Send email"]:
        try:
            submit_btn = page.get_by_role("button", name=submit_text)
            if submit_btn.is_visible(timeout=3000):
                submit_btn.click()
                page.wait_for_timeout(4000)
                print(f"  [{asin}] Case submitted successfully.")
                return True
        except Exception:
            pass

    print(f"  [{asin}] WARNING: Could not find Send button — check the browser.")
    input("  Press Enter after manually submitting, or Ctrl+C to stop...")
    return True


def log_result(asin, status, note=""):
    file_exists = False
    try:
        with open(LOG_FILE, "r"):
            file_exists = True
    except FileNotFoundError:
        pass

    with open(LOG_FILE, "a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        if not file_exists:
            writer.writerow(["Timestamp", "ASIN", "Status", "Note"])
        writer.writerow([datetime.now().strftime("%Y-%m-%d %H:%M:%S"), asin, status, note])


def main():
    asins = load_asins()
    if not asins:
        print("No ASINs found. Add them to the ASINS list or asins_to_remove.txt")
        return

    print(f"Loaded {len(asins)} ASINs.")
    print("Opening browser...")

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False, slow_mo=80)
        page = browser.new_page(viewport={"width": 1280, "height": 800})

        page.goto("https://vendorcentral.amazon.com", wait_until="domcontentloaded", timeout=60000)
        print("Browser opened. Log into Vendor Central.")
        input("Once you're logged in, press Enter here to start submitting cases...")

        success = 0
        failed = []

        for i, asin in enumerate(asins, 1):
            print(f"[{i}/{len(asins)}] Processing {asin}")
            try:
                ok = submit_case(page, asin)
                if ok:
                    success += 1
                    log_result(asin, "Submitted")
                else:
                    failed.append(asin)
                    log_result(asin, "Failed", "Submit button not found")
            except PlaywrightTimeoutError:
                print(f"  [{asin}] Timeout — skipping.")
                failed.append(asin)
                log_result(asin, "Failed", "Timeout")
            except KeyboardInterrupt:
                print("\nStopped by user.")
                log_result(asin, "Skipped", "Stopped by user")
                break

            if i < len(asins):
                print(f"  Waiting {DELAY_BETWEEN_CASES}s before next case...")
                time.sleep(DELAY_BETWEEN_CASES)

        print(f"\n{'='*50}")
        print(f"Done. {success} cases submitted, {len(failed)} failed.")
        if failed:
            print(f"Failed ASINs: {', '.join(failed)}")
        print(f"Log saved to: {LOG_FILE}")

        input("Press Enter to close the browser...")
        browser.close()


if __name__ == "__main__":
    main()
