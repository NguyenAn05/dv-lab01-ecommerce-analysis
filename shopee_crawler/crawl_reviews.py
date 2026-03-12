import os
import json
import time
import random
import pandas as pd
from datetime import datetime
from shopee_utils import connect_to_chrome, check_captcha, check_blocked, wait_captcha

OUTPUT_DIR = "shopee_data"
RAW_REVIEWS_DIR = os.path.join(OUTPUT_DIR, "raw_reviews")
REVIEW_STATE_FILE = os.path.join(OUTPUT_DIR, "review_state.json")

# Delay settings (same strategy as crawl_shops)
REVIEW_MIN_DELAY = 5
REVIEW_MAX_DELAY = 8
REST_EVERY_N = 15
REST_DURATION = (30, 60)
REVIEWS_PER_PAGE = 6  # Shopee returns 6 ratings per page

# ============================================================
# CHECKPOINT: STATE MANAGEMENT
# ============================================================

def _load_review_state() -> dict:
    if os.path.exists(REVIEW_STATE_FILE):
        try:
            with open(REVIEW_STATE_FILE, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            print(f"  Warning loading review state: {e}")
    return {
        "current_product_id": None,
        "current_product_index": 0,
        "current_page": 0,
        "total_products": 0,
        "total_reviews_crawled": 0,
        "status": "not_started"
    }

def _get_completed_products() -> set:
    """Scan raw_reviews/ folder to get set of completed product IDs."""
    completed = set()
    if os.path.exists(RAW_REVIEWS_DIR):
        for f in os.listdir(RAW_REVIEWS_DIR):
            if f.endswith('.csv'):
                try:
                    completed.add(int(f.replace('.csv', '')))
                except ValueError:
                    pass
    return completed

def _save_review_state(state: dict):
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    with open(REVIEW_STATE_FILE, 'w', encoding='utf-8') as f:
        json.dump(state, f, ensure_ascii=False, indent=2)

# ============================================================
# SCROLL + INTERCEPT: Extract get_ratings from network logs
# ============================================================

def _scroll_to_reviews(browser):
    """Scroll down to the reviews section of a product page."""
    try:
        # Try to find and scroll to review section
        found = browser.execute_script("""
            var selectors = [
                '.product-rating-overview',
                '[data-sqe="rating"]',
                '.product-ratings',
                '.shopee-product-rating'
            ];
            for (var i = 0; i < selectors.length; i++) {
                var el = document.querySelector(selectors[i]);
                if (el) {
                    el.scrollIntoView({behavior: 'smooth', block: 'center'});
                    return true;
                }
            }
            return false;
        """)
        if not found:
            # Fallback: scroll ~60% of page height
            browser.execute_script("window.scrollTo(0, document.body.scrollHeight * 0.6);")
        time.sleep(3)
    except:
        # Fallback: gradual scroll
        for _ in range(8):
            browser.execute_script("window.scrollBy(0, 500);")
            time.sleep(0.5)
        time.sleep(2)

def _click_next_review_page(browser):
    """Click the next page button in the reviews pagination. Tries multiple methods."""
    try:
        result = browser.execute_script("""
            // Helper: find the review pagination container (NOT product listing pagination)
            // It's the pagination that's near the review section (lower in page)
            function findReviewPagination() {
                // Look for pagination containers (shopee-mini-page-controller or similar)
                var paginators = document.querySelectorAll(
                    '.shopee-page-controller, .product-ratings .shopee-mini-page-controller'
                );
                // If multiple, pick the one closest to reviews (highest on page, below fold)
                var best = null;
                var bestTop = 0;
                paginators.forEach(function(p) {
                    var rect = p.getBoundingClientRect();
                    if (rect.top > 200) {  // Must be below header
                        if (!best || rect.top > bestTop) {
                            best = p;
                            bestTop = rect.top;
                        }
                    }
                });
                return best;
            }

            // === Method 1: Find "next" arrow button (.shopee-icon-button--right) ===
            var arrows = document.querySelectorAll('.shopee-icon-button--right');
            for (var i = 0; i < arrows.length; i++) {
                var rect = arrows[i].getBoundingClientRect();
                // Only arrows in review area (below fold, not product carousel arrows)
                if (rect.top > 300 && !arrows[i].disabled) {
                    arrows[i].scrollIntoView({block: 'center'});
                    arrows[i].click();
                    return 'method1-arrow';
                }
            }

            // === Method 2: Find active page number, then click next number ===
            // Look for all button elements that contain just a number
            var allButtons = document.querySelectorAll('button');
            var pageButtons = [];
            var activeIdx = -1;
            for (var i = 0; i < allButtons.length; i++) {
                var btn = allButtons[i];
                var text = btn.textContent.trim();
                var rect = btn.getBoundingClientRect();
                // Must be visible, contain only a number, and be in review area
                if (/^\\d+$/.test(text) && rect.top > 300 && rect.width > 0) {
                    pageButtons.push({el: btn, num: parseInt(text), rect: rect});
                    // Active page usually has a distinct background/class
                    var style = window.getComputedStyle(btn);
                    var bg = style.backgroundColor;
                    var classes = btn.className || '';
                    if (classes.includes('active') || 
                        bg === 'rgb(238, 77, 45)' || bg === 'rgb(255, 87, 34)' ||
                        btn.getAttribute('aria-current') === 'true') {
                        activeIdx = pageButtons.length - 1;
                    }
                }
            }

            if (activeIdx >= 0 && activeIdx < pageButtons.length - 1) {
                // Click the button right after the active one
                var nextBtn = pageButtons[activeIdx + 1];
                nextBtn.el.scrollIntoView({block: 'center'});
                nextBtn.el.click();
                return 'method2-pagenum-' + nextBtn.num;
            }

            // === Method 3: Find any right-arrow/next button in review area using SVG or > ===
            var btns = document.querySelectorAll('button');
            for (var i = 0; i < btns.length; i++) {
                var btn = btns[i];
                var rect = btn.getBoundingClientRect();
                if (rect.top < 300 || btn.disabled || rect.width === 0) continue;
                
                var hasSvg = btn.querySelector('svg');
                var text = btn.textContent.trim();
                // Could be > or â€º or SVG arrow
                if (hasSvg || text === '>' || text === 'â€º' || text === 'â†’') {
                    // Check if it's the last button in its parent (typically the "next" button)
                    var parent = btn.parentElement;
                    if (parent) {
                        var siblings = parent.querySelectorAll('button');
                        if (siblings.length >= 2 && btn === siblings[siblings.length - 1]) {
                            btn.scrollIntoView({block: 'center'});
                            btn.click();
                            return 'method3-lastbtn';
                        }
                    }
                }
            }

            // === Method 4: Find right arrow by class patterns ===
            var nextSelectors = [
                'button[class*="next"]',
                'button[class*="right"]',
                'a[class*="next"]',
                '.shopee-icon-button:last-child'
            ];
            for (var s = 0; s < nextSelectors.length; s++) {
                var els = document.querySelectorAll(nextSelectors[s]);
                for (var i = 0; i < els.length; i++) {
                    var rect = els[i].getBoundingClientRect();
                    if (rect.top > 300 && !els[i].disabled && rect.width > 0) {
                        els[i].scrollIntoView({block: 'center'});
                        els[i].click();
                        return 'method4-' + nextSelectors[s];
                    }
                }
            }

            return false;
        """)
        return result if result else False
    except Exception as e:
        print(f"\n      [CLICK ERROR] {e}")
        return False

def _extract_ratings_from_logs(browser):
    """Extract all get_ratings API responses from performance logs."""
    results = []
    log_count = 0
    matched = 0
    try:
        logs = browser.get_log('performance')
        log_count = len(logs)
        for packet in logs:
            try:
                msg = json.loads(packet.get('message', '{}')).get('message', {})
                if msg.get('method') != 'Network.responseReceived':
                    continue
                params = msg.get('params', {})
                resp_url = params.get('response', {}).get('url', '')
                if 'get_ratings' not in resp_url:
                    continue
                matched += 1
                req_id = params.get('requestId')
                resp = browser.execute_cdp_cmd('Network.getResponseBody', {'requestId': req_id})
                data = json.loads(resp.get('body', '{}'))
                results.append(data)
            except:
                continue
    except:
        pass
    return results

# ============================================================
# FETCH RATINGS VIA BROWSER (no click needed)
# ============================================================

def _fetch_ratings_page(browser, shop_id, product_id, offset, limit=6):
    """Fetch a single page of ratings using fetch() inside the browser context.
    This includes all cookies/auth automatically. Returns parsed JSON or None."""
    script = """
    var callback = arguments[arguments.length - 1];
    var url = 'https://shopee.vn/api/v4/item/get_ratings'
        + '?filter=0&flag=1&itemid=' + arguments[0]
        + '&limit=' + arguments[1]
        + '&offset=' + arguments[2]
        + '&shopid=' + arguments[3]
        + '&type=0';
    fetch(url, {credentials: 'include'})
        .then(function(r) { return r.text(); })
        .then(function(t) { callback(t); })
        .catch(function(e) { callback(JSON.stringify({error: e.message})); });
    """
    try:
        result = browser.execute_async_script(script, product_id, limit, offset, shop_id)
        return json.loads(result)
    except Exception as e:
        print(f"\n      [FETCH ERROR] {e}")
        return None

# ============================================================
# CRAWL REVIEWS FOR ONE PRODUCT (fetch-based pagination)
# ============================================================

def _crawl_reviews_for_product(browser, product_id, shop_id, limit: int = None,
                               start_page: int = 0, existing_count: int = 0,
                               existing_cmtids: set = None,
                               on_page_done=None) -> tuple:
    """Crawl reviews using fetch() API calls from within the browser.
    Writes new reviews directly to CSV (append mode) — no large list in memory.
    Returns (review_count, last_page, total_ratings, total_pages).
    Returns None as first element if captcha/block prevented crawling."""
    url = f"https://shopee.vn/product/{shop_id}/{product_id}"
    browser.get(url)
    time.sleep(5)

    # Check block/captcha after load
    if check_blocked(browser) or check_captcha(browser):
        if not wait_captcha(browser):
            return (None, 0, None, 0)  # None = captcha failed, don't mark as done
        browser.get(url)
        time.sleep(5)

    # Only keep seen IDs in memory (lightweight), not full review dicts
    seen_cmtids = set(existing_cmtids) if existing_cmtids else set()
    review_count = existing_count

    # Ensure CSV file exists with header
    _init_product_csv(product_id)

    page = start_page
    offset = start_page * REVIEWS_PER_PAGE
    total_ratings = None
    consecutive_errors = 0
    max_errors = 3
    consecutive_dup_pages = 0
    max_dup_pages = 3  # Allow skipping ahead on duplicates

    while limit is None or review_count < limit:
        data = _fetch_ratings_page(browser, shop_id, product_id, offset)

        if not data:
            # Check block/captcha
            if check_blocked(browser) or check_captcha(browser):
                print(f"\n      [p.{page+1}] BLOCKED/CAPTCHA!", end="")
                if wait_captcha(browser):
                    browser.get(url)
                    time.sleep(5)
                    continue
                else:
                    print(f"\n      [p.{page+1}] STOP: captcha failed")
                    break
            consecutive_errors += 1
            print(f"\n      [p.{page+1}] Fetch error #{consecutive_errors}/{max_errors}: no response", end="")
            if consecutive_errors >= max_errors:
                print(f"\n      [p.{page+1}] STOP: too many fetch errors")
                break
            time.sleep(3)
            continue

        # Shopee returns {"error": 0, "data": {...}} on success; non-zero = real error
        error_code = data.get('error')
        if error_code is not None and error_code != 0:
            print(f"\n      [p.{page+1}] API error: {error_code}", end="")
            if error_code in [4, 90309999]:
                consecutive_errors += 1
                if consecutive_errors >= max_errors:
                    print(f"\n      [p.{page+1}] STOP: rate limited")
                    break
                time.sleep(random.uniform(10, 20))
                continue
            consecutive_errors += 1
            if consecutive_errors >= max_errors:
                print(f"\n      [p.{page+1}] STOP: too many API errors")
                break
            time.sleep(3)
            continue

        consecutive_errors = 0
        resp_data = data.get('data', {})
        ratings = resp_data.get('ratings')
        has_more = resp_data.get('has_more', False)

        # Get total from first response
        if total_ratings is None:
            summary = resp_data.get('item_rating_summary', {})
            total_ratings = summary.get('rating_total', 0)

        n_ratings = len(ratings) if ratings else 0
        page_new_reviews = []  # Only this page's new reviews (small, will be flushed)

        if ratings:
            for rating in ratings:
                if limit is not None and review_count >= limit:
                    break

                cmtid = str(rating.get('cmtid', ''))
                if cmtid in seen_cmtids:
                    continue
                seen_cmtids.add(cmtid)

                variant_info = ''
                try:
                    p_items = rating.get('product_items', [])
                    if p_items:
                        variant_info = p_items[0].get('model_name', '')
                except:
                    pass

                page_new_reviews.append({
                    'product_id': product_id,
                    'shop_id': shop_id,
                    'cmtid': cmtid,
                    'user_name': rating.get('author_username', ''),
                    'rating_star': rating.get('rating_star', 0),
                    'comment_text': rating.get('comment', ''),
                    'created_at': datetime.fromtimestamp(rating.get('ctime', 0)).isoformat() if rating.get('ctime') else '',
                    'variant_info': variant_info,
                    'likes_count': rating.get('like_count', 0),
                    'crawled_at': datetime.now().isoformat()
                })

        new_on_page = len(page_new_reviews)

        # Flush this page's new reviews to CSV immediately
        if page_new_reviews:
            _append_reviews_to_csv(product_id, page_new_reviews)
            review_count += new_on_page
            del page_new_reviews  # Free memory right away

        print(f"\n      [p.{page+1}] {n_ratings} ratings, +{new_on_page} new, total={review_count}, has_more={has_more}", end="")

        # Callback: save checkpoint state after each page
        if on_page_done:
            on_page_done(page)

        if new_on_page == 0 and n_ratings > 0:
            consecutive_dup_pages += 1
            if consecutive_dup_pages >= max_dup_pages:
                print(f" -> STOP: {max_dup_pages} consecutive duplicate pages")
                break
            else:
                print(f" -> dup ({consecutive_dup_pages}/{max_dup_pages}), skipping ahead")
                page += 1  # Skip ahead 1 page
                offset = page * REVIEWS_PER_PAGE
                time.sleep(random.uniform(1, 2))
                continue
        else:
            consecutive_dup_pages = 0

        if not ratings:
            print(f" -> STOP: empty ratings")
            break

        if not has_more:
            print(f" -> DONE")
            break

        page += 1
        offset += REVIEWS_PER_PAGE
        time.sleep(random.uniform(1, 2))

    total_pages = (total_ratings // REVIEWS_PER_PAGE + 1) if total_ratings else page + 1
    return (review_count, page + 1, total_ratings, total_pages)

# ============================================================
# SAVE REVIEWS PER PRODUCT (individual CSV)
# ============================================================

REVIEW_CSV_COLUMNS = [
    'product_id', 'shop_id', 'cmtid', 'user_name', 'rating_star',
    'comment_text', 'created_at', 'variant_info', 'likes_count', 'crawled_at'
]

def _get_product_csv_path(product_id):
    return os.path.join(RAW_REVIEWS_DIR, f"{product_id}.csv")

def _init_product_csv(product_id):
    """Initialize a CSV file with header for a product (if doesn't exist or is empty)."""
    os.makedirs(RAW_REVIEWS_DIR, exist_ok=True)
    path = _get_product_csv_path(product_id)
    if not os.path.exists(path) or os.path.getsize(path) == 0:
        pd.DataFrame(columns=REVIEW_CSV_COLUMNS).to_csv(path, index=False, encoding='utf-8-sig')
    return path

def _append_reviews_to_csv(product_id, new_reviews: list):
    """Append new reviews to a product's CSV file (streaming write)."""
    if not new_reviews:
        return
    path = _init_product_csv(product_id)
    df = pd.DataFrame(new_reviews, columns=REVIEW_CSV_COLUMNS)
    df.to_csv(path, mode='a', header=False, index=False, encoding='utf-8-sig')

def _mark_empty_product(product_id):
    """Create an empty marker file for products with no reviews."""
    _init_product_csv(product_id)

# ============================================================
# MERGE ALL PRODUCT REVIEW FILES
# ============================================================

def _merge_all_reviews(output_file: str) -> pd.DataFrame:
    """Merge all individual review CSVs into one final file."""
    if not os.path.exists(RAW_REVIEWS_DIR):
        print("  No raw_reviews folder found.")
        return pd.DataFrame()

    all_dfs = []
    for f in os.listdir(RAW_REVIEWS_DIR):
        if not f.endswith('.csv'):
            continue
        fpath = os.path.join(RAW_REVIEWS_DIR, f)
        try:
            df = pd.read_csv(fpath)
            if not df.empty:
                all_dfs.append(df)
        except Exception as e:
            print(f"  Warning reading {f}: {e}")

    if not all_dfs:
        print("  No reviews to merge.")
        return pd.DataFrame()

    merged = pd.concat(all_dfs, ignore_index=True)
    merged = merged.drop_duplicates(subset=['product_id', 'cmtid'], keep='first')
    merged.to_csv(output_file, index=False, encoding='utf-8-sig')
    print(f"  Merged: {len(merged)} reviews from {len(all_dfs)} products -> {output_file}")
    return merged

# ============================================================
# MAIN PIPELINE
# ============================================================

def crawl_reviews_pipeline(products_file: str = None, output_file: str = None,
                           products_limit: int = None, reviews_per_product: int = None):
    if products_file is None:
        products_file = os.path.join(OUTPUT_DIR, "products.csv")
    if output_file is None:
        output_file = os.path.join(OUTPUT_DIR, "reviews.csv")

    if not os.path.exists(products_file):
        print(f"Don't have products file: {products_file}")
        return None

    products_df = pd.read_csv(products_file)
    if products_df.empty:
        print("Products file is empty")
        return None

    products_to_crawl = products_df.head(products_limit) if products_limit else products_df

    # Load checkpoint
    state = _load_review_state()

    if state["status"] == "completed":
        print(" All reviews already crawled! Merging...")
        print(" (Delete review_state.json to start fresh)")
        return _merge_all_reviews(output_file)

    # Build completed set by scanning raw_reviews/ folder (source of truth)
    os.makedirs(RAW_REVIEWS_DIR, exist_ok=True)
    completed_set = _get_completed_products()

    # If resuming mid-product, don't count it as completed
    if state["status"] == "in_progress" and state.get("current_product_id") and state.get("current_page", 0) > 0:
        completed_set.discard(state["current_product_id"])

    # Filter remaining products
    remaining = products_to_crawl[~products_to_crawl['product_id'].isin(completed_set)]
    state["total_products"] = len(products_to_crawl)
    _save_review_state(state)

    if remaining.empty:
        print(f" All {len(completed_set)} products already have review files. Merging...")
        state["status"] = "completed"
        _save_review_state(state)
        return _merge_all_reviews(output_file)

    if completed_set:
        print(f" Skip {len(completed_set)} completed products (from checkpoint).")

    # Connect browser
    browser = connect_to_chrome()
    if not browser:
        return None

    print("\n" + "="*60 + "\n CRAWL REVIEWS\n" + "="*60)
    print(f" Total: {len(products_to_crawl)} | Done: {len(completed_set)} | Remaining: {len(remaining)}")
    print(f" Reviews per product: {'All' if reviews_per_product is None else reviews_per_product}")

    if state["status"] == "in_progress":
        print(f"\n  RESUMING from checkpoint:")
        print(f"  Last product: {state.get('current_product_id')} | Page: {state.get('current_page', 0)}")
        print(f"  Completed: {len(completed_set)}/{len(products_to_crawl)}")

    # Detect partially crawled product for mid-product resume
    resume_product_id = state.get("current_product_id")
    resume_page = state.get("current_page", 0)

    resume_existing_count = 0
    resume_existing_cmtids = None

    if resume_product_id and resume_product_id not in completed_set:
        existing_csv = _get_product_csv_path(resume_product_id)
        if os.path.exists(existing_csv):
            try:
                existing_df = pd.read_csv(existing_csv)
                if not existing_df.empty:
                    resume_existing_count = len(existing_df)
                    resume_existing_cmtids = set(existing_df['cmtid'].astype(str).tolist())
                    print(f"   Resuming product {resume_product_id} from page {resume_page} ({resume_existing_count} existing reviews)")
                del existing_df  # Free memory
            except:
                resume_page = 0

    state["status"] = "in_progress"
    _save_review_state(state)

    for idx, (_, row) in enumerate(remaining.iterrows()):
        p_id = int(row['product_id'])
        s_id = int(row['shop_id'])
        p_name = str(row.get('product_name', ''))
        short_name = p_name[:35] + '...' if len(p_name) > 35 else p_name

        # Skip if already completed (double-check dedup)
        if p_id in completed_set:
            continue

        # Determine if resuming mid-product
        if p_id == resume_product_id and resume_existing_cmtids is not None:
            start_page = resume_page
            ex_count = resume_existing_count
            ex_cmtids = resume_existing_cmtids
            resume_product_id = None  # Only use once
            resume_existing_cmtids = None
        else:
            start_page = 0
            ex_count = 0
            ex_cmtids = None

        # Update state: currently crawling this product
        state["current_product_id"] = p_id
        state["current_product_index"] = len(completed_set) + 1
        state["current_page"] = start_page
        _save_review_state(state)

        page_info = f" (resume p.{start_page})" if start_page > 0 else ""
        print(f"   [{len(completed_set)+1}/{len(products_to_crawl)}] {short_name}{page_info}", end=" ")

        def _on_page_done(page_num):
            state["current_page"] = page_num
            _save_review_state(state)

        rev_count, last_page, total_ratings, total_pages = _crawl_reviews_for_product(
            browser, p_id, s_id,
            limit=reviews_per_product,
            start_page=start_page,
            existing_count=ex_count,
            existing_cmtids=ex_cmtids,
            on_page_done=_on_page_done
        )

        # rev_count=None means captcha/block prevented crawling -> skip, don't mark done
        if rev_count is None:
            print("(CAPTCHA/BLOCKED - skipped, will retry)")
            continue

        if rev_count > 0:
            new_count = rev_count - ex_count
            pages_info = f", p.{last_page}/{total_pages}" if total_pages and total_pages > 1 else ""
            total_info = f"/{total_ratings}" if total_ratings else ""
            print(f"({rev_count}{total_info} reviews{pages_info})")
            state["total_reviews_crawled"] = state.get("total_reviews_crawled", 0) + max(new_count, 0)
        else:
            _mark_empty_product(p_id)
            print("(no reviews)")

        # Mark product as completed (CSV file in raw_reviews/ is the marker)
        completed_set.add(p_id)
        state["current_page"] = 0
        _save_review_state(state)

        # Anti-blocking: periodic rest
        if idx < len(remaining) - 1:
            if (len(completed_set)) % REST_EVERY_N == 0:
                rest = random.uniform(*REST_DURATION)
                print(f"   --- Resting {rest:.0f}s after {len(completed_set)} products ---")
                time.sleep(rest)
            else:
                delay = random.uniform(REVIEW_MIN_DELAY, REVIEW_MAX_DELAY)
                time.sleep(delay)

    # All done -> merge
    state["status"] = "completed"
    state["current_product_id"] = None
    _save_review_state(state)

    print("\n" + "="*60)
    print(" MERGING ALL REVIEWS")
    print("="*60)
    final_df = _merge_all_reviews(output_file)

    print(f"\n DONE! Total reviews: {len(final_df)}")
    return final_df

