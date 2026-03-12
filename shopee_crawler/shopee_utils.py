import json
import time
import random
from selenium import webdriver
from selenium.webdriver.chrome.options import Options

# Set delay time
MIN_DELAY = 2
MAX_DELAY = 4

# Connect to Chrome with remote debugging
def connect_to_chrome():
    print("Connecting to Chrome...")
    chrome_options = Options()

    caps = {
        "browserName": "chrome",
        'goog:loggingPrefs': {'performance': 'ALL'}
    }
    for key, value in caps.items():
        chrome_options.set_capability(key, value)
    
    # Set Chrome options
    chrome_options.add_experimental_option('debuggerAddress', 'localhost:9222')
    
    # Connect to Chrome
    try:
        browser = webdriver.Chrome(options=chrome_options)
        print("Connected to Chrome successfully!")
        return browser
    except Exception as e:
        print(f"Connection error: {e}")
        print("Please open Chrome with: chrome.exe --remote-debugging-port=9222 --user-data-dir=C:\\chrome_debug_profile")
        return None

# Random delay between requests
def random_delay():
    delay = random.uniform(MIN_DELAY, MAX_DELAY)
    time.sleep(delay)
    return delay

# Check CAPTCHA or traffic block
def check_captcha(browser) -> bool:
    try:
        url = browser.current_url
        if url is None:
            return False
        url_lower = url.lower()
        return 'captcha' in url_lower or 'verify' in url_lower
    except Exception:
        return False

def check_blocked(browser) -> bool:
    """Check if Shopee blocked due to too many requests"""
    try:
        url = browser.current_url
        if url is None:
            return False
        return 'verify/traffic/error' in url.lower()
    except Exception:
        return False

# Wait CAPTCHA or block
def wait_captcha(browser, timeout=120):
    if not check_captcha(browser) and not check_blocked(browser):
        return True
    
    if check_blocked(browser):
        print("\n  BLOCKED! Shopee rate limit. Waiting 60s then click 'Trở về trang chủ'...")
        time.sleep(60)
        try:
            browser.get('https://shopee.vn')
            time.sleep(5)
        except:
            pass
        return not check_blocked(browser)
    
    print("\n  CAPTCHA! Please solve in browser...")
    start = time.time()
    while time.time() - start < timeout:
        if not check_captcha(browser):
            print("CAPTCHA solved!")
            time.sleep(2)
            return True
        time.sleep(2)
    return False

# Get API response
def get_api_response(browser, url: str, api_pattern: str) -> dict:
    try:
        browser.get(url)
    except Exception as e:
        print(f"Error loading page: {e}")
        return None
    time.sleep(5)
    
    # Check block/CAPTCHA after page load
    if check_blocked(browser) or check_captcha(browser):
        if not wait_captcha(browser):
            return None
        try:
            browser.get(url)
        except Exception as e:
            print(f"Error reloading page: {e}")
            return None
        time.sleep(5)
    
    # Scroll to ensure API calls are triggered
    try:
        browser.execute_script("window.scrollBy(0, 300);")
        time.sleep(2)
    except:
        pass
    
    # Check CAPTCHA after scroll
    if check_captcha(browser):
        if not wait_captcha(browser):
            return None
        try:
            browser.get(url)
        except Exception as e:
            print(f"Error reloading page: {e}")
            return None
        time.sleep(5)
    
    # Get logs
    try:
        logs = browser.get_log('performance')
    except Exception as e:
        print(f"Error getting logs: {e}")
        return None
    
    # Parse logs
    for packet in logs:
        try:
            msg = json.loads(packet.get('message', '{}')).get('message', {})
            if msg.get('method') != 'Network.responseReceived':
                continue
            
            params = msg.get('params', {})
            resp_url = params.get('response', {}).get('url', '')
            
            # Check if API pattern is in response URL
            if api_pattern not in resp_url:
                continue
            
            # Get response body
            req_id = params.get('requestId')
            resp = browser.execute_cdp_cmd('Network.getResponseBody', {'requestId': req_id})
            return json.loads(resp.get('body', '{}'))
            
        except:
            continue
    
    return None
