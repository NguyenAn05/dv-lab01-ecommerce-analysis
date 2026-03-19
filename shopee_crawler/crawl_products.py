import os
import re
import json
import pandas as pd
from datetime import datetime
from shopee_utils import connect_to_chrome, get_api_response, random_delay
from selenium.webdriver.common.by import By

# ============================================================
# CATEGORY CONFIGURATION
# ============================================================
CATEGORIES = [
    {
        "name": "Thời trang Nam", 
        "catid": "11035567",
        "sub_categories": [
            {"name": "Áo Khoác", "catid": "11035568"},
            {"name": "Áo Vest và Blazer", "catid": "11035572"},
            {"name": "Áo Hoddie, Áo Len & Áo Nỉ", "catid": "11035578"},
            {"name": "Quần Jeans", "catid": "11035583"},
            {"name": "Quần Dài/Quần Âu", "catid": "11035584"},
            {"name": "Quần Short", "catid": "11035590"},
            {"name": "Áo", "catid": "11035592"},
            {"name": "Áo Ba Lỗ", "catid": "11035597"},
            {"name": "Đồ Lót", "catid": "11035598"},
            {"name": "Đồ Ngủ", "catid": "11035603"},
            {"name": "Đồ Bộ", "catid": "11035604"},
            {"name": "Vớ Tất", "catid": "11035605"},
            {"name": "Trang Phục Truyền Thống", "catid": "11035606"},
            {"name": "Đồ Hóa Trang", "catid": "11035611"},
            {"name": "Trang Phục Ngành Nghề", "catid": "11035612"},
            {"name": "Khác", "catid": "11035613"},
            {"name": "Trang Sức Nam", "catid": "11035614"},
            {"name": "Kính Mắt Nam", "catid": "11035620"},
            {"name": "Thắt Lưng Nam", "catid": "11035625"},
            {"name": "Cà vạt & Nơ cổ", "catid": "11035626"},
            {"name": "Phụ Kiện Nam", "catid": "11035627"}
        ]
    },
    {
        "name": "Thời trang Nữ", 
        "catid": "11035639",
        "sub_categories": [
            {"name": "Quần", "catid": "11035648"},
            {"name": "Quần đùi", "catid": "11035652"},
            {"name": "Chân váy", "catid": "11035656"},
            {"name": "Quần jeans", "catid": "11035657"},
            {"name": "Đầm/Váy", "catid": "11035658"},
            {"name": "Váy cưới", "catid": "11035659"},
            {"name": "Đồ liền thân", "catid": "11035660"},
            {"name": "Áo khoác, Áo choàng & Vest", "catid": "11035665"},
            {"name": "Áo len & Cardigan", "catid": "11035672"},
            {"name": "Hoodie và Áo nỉ", "catid": "11035673"},
            {"name": "Bộ", "catid": "11035677"},
            {"name": "Đồ lót", "catid": "11035682"},
            {"name": "Đồ ngủ", "catid": "11035692"},
            {"name": "Áo", "catid": "11035640"},
            {"name": "Đồ tập", "catid": "11035730"},
            {"name": "Đồ Bầu", "catid": "11035697"},
            {"name": "Đồ truyền thống", "catid": "11035705"},
            {"name": "Đồ hóa trang", "catid": "11035711"},
            {"name": "Vải", "catid": "11035713"},
            {"name": "Vớ/ Tất", "catid": "11035726"},
            {"name": "Khác", "catid": "11035712"}
        ]
    }
]

# Output directory
OUTPUT_DIR = "../data/raw"
MAX_PAGES = None # Will be set by crawl_products_pipeline()

# ============================================================
# CHECKPOINT: STATE MANAGEMENT
# ============================================================

STATE_FILE = os.path.join(OUTPUT_DIR, "state.json")

# Load checkpoint state from state.json
def _load_state() -> dict:
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            print(f"  Warning: {e}")
    return {
        "completed_sub_categories": [],
        "completed_categories": [],
        "current_category": None,
        "current_sub_category": None,
        "current_page": 0,
        "status": "not_started"
    }

# Save checkpoint state to state.json
def _save_state(state: dict):
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    with open(STATE_FILE, 'w', encoding='utf-8') as f:
        json.dump(state, f, ensure_ascii=False, indent=2)

# Convert Vietnamese name to safe filename
def _safe_filename(name: str) -> str:
    name = re.sub(r'[/\\,&\s]+', '_', name)
    name = re.sub(r'[^\w\-]', '', name)
    name = name.strip('_').lower()
    return name

# Convert sub-category name to Shopee URL slug
def _url_slug(name: str) -> str:
    slug = re.sub(r'[,&/\\]+', ' ', name)
    slug = re.sub(r'\s+', '-', slug.strip())
    return slug

# ============================================================
# PARSE PRODUCT ITEM (recommend_v2 API structure)
# ============================================================

def _parse_sold_text(text: str) -> int:
    """Parse Shopee sold text: 'Đã bán 10k+' → 10000, 'Đã bán 1.2k+' → 1200"""
    if not text:
        return 0
    match = re.search(r'([\d.,]+)\s*(k|tr|m)?', text.lower())
    if not match:
        return 0
    num_str = match.group(1).replace(',', '.')
    num = float(num_str)
    suffix = match.group(2)
    if suffix == 'k':
        num *= 1000
    elif suffix in ('tr', 'm'):
        num *= 1000000
    return int(num)

def _parse_product_item(unit: dict, category: str, sub_category: str) -> dict:
    item = unit.get('item', {})
    item_data = item.get('item_data', {})
    asset = item.get('item_card_displayed_asset', {})
    
    # Price
    price_info = item_data.get('item_card_display_price', {})
    price = (price_info.get('price', 0) or 0) / 100000
    strikethrough = (price_info.get('strikethrough_price', 0) or 0) / 100000
    discount = price_info.get('discount', 0) or 0
    
    # Rating
    rating_info = item_data.get('item_rating', {})
    rating_star = rating_info.get('rating_star', 0) if rating_info else 0
    
    # Sold count (text → number)
    sold_info = item_data.get('item_card_display_sold_count', {})
    sold_text = (sold_info.get('historical_sold_count_text', '') or '') if sold_info else ''
    historical_sold = _parse_sold_text(sold_text)
    
    shop_info = item_data.get('shop_data', {}) or {}
    
    return {
        'product_id': item_data.get('itemid'),
        'shop_id': item_data.get('shopid'),
        'shop_location': shop_info.get('shop_location', ''),
        'product_name': asset.get('name', ''),
        'category': category,
        'sub_category': sub_category,
        'original_price': strikethrough if strikethrough > 0 else price,
        'current_price': price,
        'discount_percentage': discount,
        'historical_sold': historical_sold,
        'rating_average': rating_star,
        'is_sold_out': item_data.get('is_sold_out', False),
        'liked_count': item_data.get('liked_count', 0),
        'crawled_at': datetime.now().isoformat()
    }

# ============================================================
# CRAWL ONE SUB-CATEGORY (with page-level checkpoint)
# ============================================================

# Crawl all pages of one sub-category
def _crawl_sub_category(browser, cat_name, cat_id, sub_cat_name, sub_cat_id, state, start_page=0):
    cat_folder = _safe_filename(cat_name)
    sub_file = _safe_filename(sub_cat_name) + ".csv"
    raw_dir = os.path.join(OUTPUT_DIR, "raw", cat_folder)
    os.makedirs(raw_dir, exist_ok=True)
    raw_path = os.path.join(raw_dir, sub_file)
    
    # Load existing products if resuming mid-sub-category
    all_products = []
    seen_ids = set()
    if start_page > 0 and os.path.exists(raw_path):
        try:
            existing_df = pd.read_csv(raw_path)
            all_products = existing_df.to_dict('records')
            seen_ids = {p['product_id'] for p in all_products if p.get('product_id')}
            print(f"    Resuming from page {start_page}, {len(all_products)} products loaded")
        except Exception as e:
            print(f"  Warning: {e}")
    
    # Crawl products
    page = start_page
    total_pages = None
    
    while True:
        # Check MAX_PAGES limit
        if MAX_PAGES is not None and page >= MAX_PAGES:
            print(f"    Reached MAX_PAGES limit ({MAX_PAGES})")
            break
        
        # Check total_pages (calculated from API response)
        if total_pages is not None and page >= total_pages:
            print(f"    All {total_pages} pages crawled")
            break
        
        # Update state: current page
        state["current_page"] = page
        _save_state(state)
        
        # Call API and check data
        slug = _url_slug(sub_cat_name)
        url = f"https://shopee.vn/{slug}-cat.{cat_id}.{sub_cat_id}?page={page}"
        progress = f"Page {page + 1}" + (f"/{total_pages}" if total_pages else "") + "..."
        print(f"    {progress}", end=" ")
        
        data = get_api_response(browser, url, 'api/v4/recommend/recommend_v2')
        
        if not data:
            print("No data")
            break
        
        # Extract units from new API structure: data.data.units
        api_data = data.get('data', {})
        units = api_data.get('units', [])
        
        # Calculate total pages from UI pagination (more accurate than API total)
        if total_pages is None:
            try:
                pagination_el = browser.find_element(By.CSS_SELECTOR, ".shopee-mini-page-controller__total")
                total_pages = int(pagination_el.text)
                if MAX_PAGES is not None:
                    total_pages = min(total_pages, MAX_PAGES)
                print(f"[{total_pages} pages from UI] ", end="")
            except Exception:
                # Fallback: use API total
                total_count = api_data.get('total', 0)
                items_per_page = 60
                if total_count > 0:
                    total_pages = (total_count // items_per_page) + 1
                    if MAX_PAGES is not None:
                        total_pages = min(total_pages, MAX_PAGES)
                    print(f"[~{total_pages} pages from API] ", end="")
        
        if not units:
            print("Empty page - end of results")
            break
        
        # Parse products (skip duplicates by product_id)
        new_count = 0
        for unit in units:
            parsed = _parse_product_item(unit, cat_name, sub_cat_name)
            pid = parsed.get('product_id')
            if pid and pid not in seen_ids:
                seen_ids.add(pid)
                all_products.append(parsed)
                new_count += 1
        
        print(f"{new_count} new / {len(units)} items (total: {len(all_products)})")
        
        # Save after each page
        df = pd.DataFrame(all_products)
        df.to_csv(raw_path, index=False, encoding='utf-8-sig')
        
        page += 1
        random_delay()
    
    if MAX_PAGES is not None and total_pages is not None and MAX_PAGES < total_pages:
        print(f"    Limited: {MAX_PAGES}/{total_pages} pages ({round(MAX_PAGES/total_pages*100)}%)")
    print(f"    Sub-category done: {len(all_products)} products -> {raw_path}")
    return all_products

# ============================================================
# MERGE FUNCTIONS
# ============================================================

# Merge all sub-category CSVs into one category CSV
def _merge_category(cat_name: str) -> pd.DataFrame:
    cat_folder = _safe_filename(cat_name)
    raw_dir = os.path.join(OUTPUT_DIR, "raw", cat_folder)
    cat_output = os.path.join(OUTPUT_DIR, f"products_{cat_folder}.csv")
    
    if not os.path.exists(raw_dir):
        print(f"  No data folder found for {cat_name}")
        return pd.DataFrame()
    
    all_dfs = []
    for f in sorted(os.listdir(raw_dir)):
        if f.endswith('.csv'):
            fpath = os.path.join(raw_dir, f)
            try:
                df = pd.read_csv(fpath)
                all_dfs.append(df)
            except Exception as e:
                print(f"  Warning: {e}")
    
    if not all_dfs:
        return pd.DataFrame()
    
    merged = pd.concat(all_dfs, ignore_index=True)
    before = len(merged)
    merged = merged.drop_duplicates(subset=['product_id'], keep='first')
    after = len(merged)
    if before > after:
        print(f"  Deduplicated: {before} -> {after} ({before - after} duplicates removed)")
    merged.to_csv(cat_output, index=False, encoding='utf-8-sig')
    print(f"  Merged {cat_name}: {after} products -> {cat_output}")
    return merged

# Merge all category CSVs into final products.csv
def _merge_all_categories() -> pd.DataFrame:
    final_output = os.path.join(OUTPUT_DIR, "products.csv")
    
    all_dfs = []
    for f in os.listdir(OUTPUT_DIR):
        if f.startswith('products_') and f.endswith('.csv'):
            fpath = os.path.join(OUTPUT_DIR, f)
            try:
                df = pd.read_csv(fpath)
                all_dfs.append(df)
            except Exception as e:
                print(f"  Warning: {e}")
    
    if not all_dfs:
        return pd.DataFrame()
    
    final = pd.concat(all_dfs, ignore_index=True)
    before = len(final)
    final = final.drop_duplicates(subset=['product_id'], keep='first')
    after = len(final)
    if before > after:
        print(f"  Deduplicated: {before} -> {after} ({before - after} duplicates removed)")
    final.to_csv(final_output, index=False, encoding='utf-8-sig')
    print(f"\n  FINAL: {after} products -> {final_output}")
    return final

# ============================================================
# MAIN PIPELINE
# ============================================================

def crawl_products_pipeline(max_pages: int = None):
    global MAX_PAGES
    MAX_PAGES = max_pages
    
    # Load checkpoint
    state = _load_state()
    
    if state["status"] == "completed":
        print("All products have already been crawled!")
        print("Delete state.json to start fresh, or just use products.csv")
        final_path = os.path.join(OUTPUT_DIR, "products.csv")
        if os.path.exists(final_path):
            return pd.read_csv(final_path)
        return None
    
    # Connect browser
    browser = connect_to_chrome()
    if not browser:
        return None
    
    print("\n" + "="*60)
    print(" CRAWL PRODUCTS (with checkpoint)")
    print("="*60)
    
    if state["status"] == "in_progress":
        print(f"\n  RESUMING from checkpoint:")
        print(f"  Category: {state['current_category']}")
        print(f"  Sub-category: {state['current_sub_category']}")
        print(f"  Page: {state['current_page']}")
        print(f"  Completed sub-cats: {len(state['completed_sub_categories'])}")
    
    state["status"] = "in_progress"
    _save_state(state)
    
    # Crawl each category
    for cat in CATEGORIES:
        cat_name = cat["name"]
        
        # Skip completed categories
        if cat_name in state["completed_categories"]:
            print(f"\n  [SKIP] {cat_name} (already completed)")
            continue
        
        print(f"\n{'='*60}")
        print(f"  CATEGORY: {cat_name}")
        print(f"{'='*60}")
        
        state["current_category"] = cat_name
        
        for sub_cat in cat.get("sub_categories", []):
            sub_cat_name = sub_cat["name"]
            sub_cat_id = sub_cat["catid"]
            
            # Skip completed sub-categories
            if sub_cat_id in state["completed_sub_categories"]:
                print(f"\n  [SKIP] {cat_name} > {sub_cat_name}")
                continue
            
            print(f"\n  {cat_name} > {sub_cat_name}")
            
            # Determine start page (resume if this was the interrupted sub-cat)
            start_page = 0
            if (state["current_sub_category"] == sub_cat_name 
                and state["current_category"] == cat_name
                and state["current_page"] > 0):
                start_page = state["current_page"]
            
            state["current_sub_category"] = sub_cat_name
            _save_state(state)
            
            # Crawl this sub-category
            _crawl_sub_category(browser, cat_name, cat["catid"], sub_cat_name, sub_cat_id, state, start_page)
            
            # Mark sub-category as completed
            if sub_cat_id not in state["completed_sub_categories"]:
                state["completed_sub_categories"].append(sub_cat_id)
            state["current_page"] = 0
            _save_state(state)
        
        # All sub-categories for this category done -> merge
        print(f"\n  Merging {cat_name}...")
        _merge_category(cat_name)
        
        if cat_name not in state["completed_categories"]:
            state["completed_categories"].append(cat_name)
        _save_state(state)
    
    # All categories done -> final merge
    print("\n" + "="*60)
    print(" MERGING ALL CATEGORIES")
    print("="*60)
    
    final_df = _merge_all_categories()
    
    state["status"] = "completed"
    state["current_category"] = None
    state["current_sub_category"] = None
    state["current_page"] = 0
    _save_state(state)

    print(f"\n  DONE! Total products: {len(final_df)}")
    return final_df
