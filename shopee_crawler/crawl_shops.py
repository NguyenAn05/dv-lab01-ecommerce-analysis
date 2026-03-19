import os
import time
import random
import pandas as pd
from datetime import datetime
from shopee_utils import connect_to_chrome, get_api_response, random_delay, check_blocked

OUTPUT_DIR = "../data/raw"

# Longer delay for shop crawling to avoid rate limit
SHOP_MIN_DELAY = 5
SHOP_MAX_DELAY = 8
REST_EVERY_N = 10      # Rest after every N shops
REST_DURATION = (30, 60)  # Rest duration range in seconds

# Main pipeline to crawl shop details based on product data
def crawl_shops_pipeline(products_file: str = None, output_file: str = None, limit: int = None):
    if products_file is None:
        products_file = os.path.join(OUTPUT_DIR, "products.csv")
    if output_file is None:
        output_file = os.path.join(OUTPUT_DIR, "shops.csv")

    # Read products data
    if not os.path.exists(products_file):
        print(f"Don't have products file: {products_file}")
        return None
    
    products_df = pd.read_csv(products_file)
    if products_df.empty:
        print(f"Products file is empty")
        return None

    # Get unique shop IDs
    shop_ids = products_df['shop_id'].unique().tolist()
    if limit:
        shop_ids = shop_ids[:limit]

    # Check existing shops to avoid re-crawling
    existing_shops = set()
    if os.path.exists(output_file):
        try:
            old_shops_df = pd.read_csv(output_file)
            existing_shops = set(old_shops_df['shop_id'].unique())
            print(f" Skip {len(existing_shops)} existing shops from previous crawl.")
        except Exception as e:
            print(f" Error reading existing shops file: {e}")

    shop_ids = [s for s in shop_ids if s not in existing_shops]

    if not shop_ids:
        print(" All shops in the list have already been crawled. No need to crawl more.")
        return pd.read_csv(output_file) if os.path.exists(output_file) else None

    # Build shop_location fallback from products data
    location_map = {}
    if 'shop_location' in products_df.columns:
        loc_df = products_df.dropna(subset=['shop_location'])
        loc_df = loc_df[loc_df['shop_location'] != '']
        location_map = loc_df.groupby('shop_id')['shop_location'].first().to_dict()

    # Connect to Chrome
    browser = connect_to_chrome()
    if not browser:
        return None

    print("\n" + "="*60 + "\n CRAWL SHOP DETAILS\n" + "="*60)
    print(f" Cần crawl mới: {len(shop_ids)} shop")
    
    new_shops = []
    
    # Crawl shop details
    for i, shop_id in enumerate(shop_ids):
        print(f"   [{i+1}/{len(shop_ids)}] Shop ID: {shop_id}", end=" ")
        
        url = f"https://shopee.vn/shop/{shop_id}"
        data = get_api_response(browser, url, 'api/v4/shop/get_shop_base')
        if not data:
            data = get_api_response(browser, url, 'api/v4/shop/get_shop_detail')
        
        if data and data.get('data'):
            shop_data = data.get('data', {})
            
            api_location = shop_data.get('shop_location', '')
            shop = {
                'shop_id': shop_id,
                'shop_name': shop_data.get('name', shop_data.get('shop_name', '')),
                'location': api_location if api_location else location_map.get(shop_id, ''),
                'rating_star': shop_data.get('rating_star', 0),
                'follower_count': shop_data.get('follower_count', 0),
                'is_official_shop': shop_data.get('is_official_shop', False),
                'item_count': shop_data.get('item_count', 0),
                'response_rate': shop_data.get('response_rate', 0),
                'response_time': shop_data.get('response_time', 0),
                'crawled_at': datetime.now().isoformat()
            }
            print("")
        else:
            # Fallback
            shop = {
                'shop_id': shop_id,
                'shop_name': '',
                'location': location_map.get(shop_id, ''),
                'rating_star': 0,
                'follower_count': 0,
                'is_official_shop': False,
                'item_count': 0,
                'response_rate': 0,
                'response_time': 0,
                'crawled_at': datetime.now().isoformat()
            }
            print(" (no API data)")
        
        new_shops.append(shop)
        
        # Save after each shop to ensure progress is not lost
        df_temp = pd.DataFrame([shop])
        df_temp.to_csv(output_file, mode='a', header=not os.path.exists(output_file), index=False, encoding='utf-8-sig')
        
        if i < len(shop_ids) - 1:
            # Periodic long rest to avoid rate limit
            if (i + 1) % REST_EVERY_N == 0:
                rest = random.uniform(*REST_DURATION)
                print(f"   --- Resting {rest:.0f}s after {i+1} shops to avoid block ---")
                time.sleep(rest)
            else:
                # Longer delay between shops (5-8s instead of 2-4s)
                delay = random.uniform(SHOP_MIN_DELAY, SHOP_MAX_DELAY)
                time.sleep(delay)

    final_df = pd.read_csv(output_file)
    print(f"\n Finished crawling shops. Total shops in file: {len(final_df)}")
    return final_df
