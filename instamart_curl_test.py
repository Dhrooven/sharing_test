import gspread
from oauth2client.service_account import ServiceAccountCredentials
import time
import random
# fake_useragent is not strictly needed if we are setting a specific UA,
# but keeping it for now in case of future randomization needs.
from fake_useragent import UserAgent
import json
from curl_cffi import requests as curl_requests # Using curl_cffi

# --- CONFIGURATION ---
GOOGLE_SHEET_NAME = "KW RANKING"
WORKSHEET_NAME = "StoreIDs"
# IMPORTANT: Replace with your actual path to credentials.json
GOOGLE_CREDENTIALS_FILE = "/home/dj/Desktop/Programming/Bajo_Foods/Scraper_Finals/credentials.json"

# Swiggy API Configuration
API_BASE_URL = "https://www.swiggy.com/api/instamart/search"
DEFAULT_PARAMS = { # These will be URL parameters
    "pageNumber": "0",
    "searchResultsOffset": "0",
    "limit": "40", # Updated from 100 to 40 (matches browser log)
    "ageConsent": "false",
    "layoutId": "2671", # Updated from "" (matches browser log)
    "pageType": "INSTAMART_PRE_SEARCH_PAGE", # Updated (matches browser log)
    "isPreSearchTag": "false",
    "highConfidencePageNo": "0",
    "lowConfidencePageNo": "0",
    "voiceSearchTrackingId": "",
    # storeId, primaryStoreId, secondaryStoreId will be added from sheet/logic
}

# Google Sheet Column/Row Configuration
LOCATION_ROW = 2
LOCATION_COL = 4
HEADER_ROW = 3
KEYWORD_COL = 1
PRODUCT_NAME_COL = 2
SKU_ID_COL = 3
RANK_COL = 4
DATA_START_ROW = 4

# Anti-bot settings
MIN_DELAY_SECONDS = 10 # Slightly increased
MAX_DELAY_SECONDS = 28 # Slightly increased
USE_PROXY = False # Keep False unless you have reliable, rotating proxies
PROXY_CONFIG = {"http": None, "httpsS": None}
# Updated to match browser log (Chrome 136)
IMPERSONATE_BROWSER = "chrome136"

# --- END CONFIGURATION ---

ua = UserAgent()

def get_google_sheet_worksheet():
    """Authenticates with Google Sheets API and returns the specified worksheet."""
    try:
        scope = ["https://spreadsheets.google.com/feeds", 'https://www.googleapis.com/auth/spreadsheets',
                 "https://www.googleapis.com/auth/drive.file", "https://www.googleapis.com/auth/drive"]
        creds = ServiceAccountCredentials.from_json_keyfile_name(GOOGLE_CREDENTIALS_FILE, scope)
        client = gspread.authorize(creds)
        sheet = client.open(GOOGLE_SHEET_NAME).worksheet(WORKSHEET_NAME)
        print(f"Successfully connected to Google Sheet: '{GOOGLE_SHEET_NAME}' -> Worksheet: '{WORKSHEET_NAME}'")
        return sheet
    except Exception as e:
        print(f"Error connecting to Google Sheets: {e}")
        return None

def get_request_headers(keyword, store_id_for_header=None):
    """
    Returns a dictionary of headers, attempting to mimic a browser.
    Uses some static values from the provided successful log.
    WARNING: aws-waf-token and matcher are likely dynamic and session-specific.
             Hardcoding them will likely lead to eventual failure.
    """
    # Specific User-Agent from the successful browser log
    specific_user_agent = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36"

    headers = {
        "User-Agent": specific_user_agent,
        "Accept": "*/*", # Changed from application/json, text/plain, */*
        "Accept-Language": "en-GB,en-US;q=0.9,en;q=0.8",
        "Accept-Encoding": "gzip, deflate, br, zstd", # curl_cffi handles actual encoding
        "Content-Type": "application/json",
        "Origin": "https://www.swiggy.com",
        # Dynamic Referer based on the search query
        "Referer": f"https://www.swiggy.com/instamart/search?query={keyword}",
        "DNT": "1",
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-origin",
        # "X-Requested-With": "XMLHttpRequest", # Not in provided log's top headers
        "x-build-version": "2.273.0", # Updated from 2.272.0 (matches browser log)
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
        # matcher from browser log. HIGHLY LIKELY DYNAMIC.
        "matcher": "dd7dc8ebeb97ecafedgcdf7",
        # aws-waf-token from browser log. EXTREMELY DYNAMIC AND SHORT-LIVED.
        # This is the most common reason for WAF blocks.
        "aws-waf-token": "ac4eabe1-2c74-4b11-af7d-a7e7eb9cf7e6:BQoAvSKBqoc4AQAA:TrgzZZJikZcR/5RgfPxRGLvzgQvP8nqaiSSXNKBF5XpYIW48/qp6gaRkYsk/QjpZBz7nTdO2Rht7SLcxwu2jRRkpVYRCvgmO2w9mSvWoZZmMMgkkD5VyS7l0z+irdYeziqnEsvs2c6bmDQmQuHT6MTsNCTBHzeg6NlCJjAmI2Iea1GX0opClrP3y3b2jw2sj5NY/NdfIIva9cmQ4w/wpLj9RBR2UOmnECuOdChy2PdiTaE1KA5AOpNI2wr1JtEtopCI7T5E=",
        # Sec-CH-UA headers from browser log
        "sec-ch-ua": '"Chromium";v="136", "Google Chrome";v="136", "Not.A/Brand";v="99"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"Linux"',
        "priority": "u=1, i", # Added from browser log
    }
    # Removed Swiggy specific headers like __SWIGGY_CLIENT_ID__ as they were not in the top-level headers
    # of the provided successful request log. `impersonate` might handle similar aspects.
    return headers

def fetch_search_results(session, keyword, store_id_from_sheet):
    """Fetches search results from Swiggy Instamart API with detailed logging."""
    
    url_params = DEFAULT_PARAMS.copy()
    url_params["query"] = keyword
    if not store_id_from_sheet:
        print(f"ERROR: Store ID is missing for keyword '{keyword}'. Cannot proceed.")
        return None
    url_params["storeId"] = store_id_from_sheet
    url_params["primaryStoreId"] = store_id_from_sheet
    url_params["secondaryStoreId"] = "" # Added as per browser log (empty value)

    post_data_payload = {
        "facets": {},
        "sortAttribute": ""
    }

    api_url = API_BASE_URL
    proxies = PROXY_CONFIG if USE_PROXY and PROXY_CONFIG.get("httpsS") else None
    request_headers = get_request_headers(keyword, store_id_from_sheet)

    print(f"\n--- Request for Keyword: '{keyword}', Store ID: '{store_id_from_sheet}' ---")
    print(f"URL: {api_url}")
    print(f"URL Params: {url_params}")
    print(f"JSON Body: {json.dumps(post_data_payload)}")
    print(f"Request Headers (selected):")
    print(f"  User-Agent: {request_headers.get('User-Agent')}")
    print(f"  Referer: {request_headers.get('Referer')}")
    print(f"  matcher: {request_headers.get('matcher')}")
    print(f"  aws-waf-token: {request_headers.get('aws-waf-token')[:30]}...") # Print only start of token
    # print(f"Request Headers (Full): {json.dumps(request_headers, indent=2)}") # Uncomment for full header debug

    try:
        response = session.post(
            api_url,
            params=url_params,
            json=post_data_payload,
            headers=request_headers,
            proxies=proxies,
            timeout=30, # Increased timeout slightly
            impersonate=IMPERSONATE_BROWSER
        )
        
        print("\n--- Response Details ---")
        print(f"Status Code: {response.status_code}")
        # print("Response Headers:") # Can be verbose, enable if needed
        # for h_name, h_value in response.headers.items():
        #     print(f"  {h_name}: {h_value}")
        
        print(f"Response Content-Type: {response.headers.get('Content-Type', 'None')}")
        print(f"Response Content-Length: {response.headers.get('Content-Length', 'N/A')}")

        # print("\nSession Cookies After Request:") # Enable for debugging cookie persistence
        # if session.cookies:
        #     for cookie_name, cookie_value in session.cookies.items():
        #         print(f"  {cookie_name}: {cookie_value}")
        # else:
        #     print("  No cookies in session.")

        # Check for typical WAF indicators in headers if response is not as expected
        if response.status_code == 200 and response.headers.get('Content-Length') == '0':
            print("Warning: Received 200 OK with Content-Length 0. This might indicate a WAF block or an issue with the request structure.")
            print(f"  X-Cache: {response.headers.get('x-cache')}")
            print(f"  Via: {response.headers.get('via')}")


        content_type = response.headers.get("Content-Type", "").lower()
        if "application/json" not in content_type:
            print(f"\nWarning: Response for '{keyword}' is NOT JSON. Actual Content-Type: '{response.headers.get('Content-Type', 'None')}'")
            print(f"Response Text (first 500 chars):\n{response.text[:500]}")
            # If content length is 0, it's likely the WAF, and raise_for_status might not trigger if 200 OK
            if response.headers.get('Content-Length') == '0' or not response.text.strip():
                 print("Empty response body received.")
                 return None # Explicitly return None for empty body
            # Fallthrough to raise_for_status for other non-JSON cases with content
        
        response.raise_for_status() # Will raise an HTTPError for bad responses (4xx or 5xx)
        
        # Attempt to parse JSON only if content_type suggests it and there's content
        if response.text.strip():
            print("Response successfully parsed as JSON.")
            return response.json()
        else:
            print("Response body is empty, cannot parse as JSON.")
            return None
            
    except curl_requests.RequestsError as e:
        print(f"\ncurl_cffi request error for keyword '{keyword}': {e}")
        if hasattr(e, 'response') and e.response is not None:
            print(f"  Response status: {e.response.status_code}, Response text: {e.response.text[:200]}")
        else:
            print(f"  No response object available for error: {e}")
    except json.JSONDecodeError:
        print(f"\nFailed to decode JSON response for keyword '{keyword}'.")
        # print(f"Response Status: {response.status_code}") # Already printed
        # print(f"Response Text (first 500 chars):\n{response.text[:500]}") # Already printed if not JSON
    except Exception as e:
        print(f"\nAn unexpected error occurred while fetching data for '{keyword}': {e}")
        import traceback
        traceback.print_exc()
    return None

def find_sku_rank(api_response, sku_id_to_find):
    """Parses the API response to find the rank of a given SKU ID."""
    if not api_response: # This check is now more critical
        # fetch_search_results should have logged why it's None
        print(f"  Skipping SKU rank parsing for SKU {sku_id_to_find} due to missing API response.")
        return "API Error/No Data", None 
    
    print("\n--- Parsing SKU Rank ---")
    try:
        if 'data' not in api_response or not isinstance(api_response['data'], dict):
            print(f"Missing 'data' key or 'data' is not a dictionary for SKU {sku_id_to_find}.")
            print(f"API Response keys: {list(api_response.keys()) if isinstance(api_response, dict) else 'Not a dict'}")
            return "Parse Error (no data dict)", None

        widgets = api_response['data'].get('widgets', [])
        if not widgets or not isinstance(widgets, list):
            print(f"Could not find 'widgets' list or it's not a list for SKU {sku_id_to_find}.")
            return "Parse Error (no widgets list)", None

        product_items = []
        for widget_idx, widget in enumerate(widgets):
            if isinstance(widget, dict):
                widget_info = widget.get("widgetInfo", {})
                items_in_widget_data = widget.get('data')

                if isinstance(widget_info, dict) and widget_info.get("widgetType") == "PRODUCT_LIST":
                    if isinstance(items_in_widget_data, list):
                        product_items.extend(items_in_widget_data)
                elif isinstance(items_in_widget_data, list): # Fallback for other widget structures
                    if items_in_widget_data and isinstance(items_in_widget_data[0], dict) and \
                       any(key in items_in_widget_data[0] for key in ['product_id', 'variations', 'display_name']):
                        product_items.extend(items_in_widget_data)
        
        if not product_items:
            print(f"No product items found after checking all widgets for SKU {sku_id_to_find}.")
            return "Parse Error (no items in widgets)", None
        
        sku_id_to_find_str = str(sku_id_to_find)
        for index, item_container in enumerate(product_items):
            if not isinstance(item_container, dict):
                continue

            current_sku_primary = str(item_container.get('product_id', ''))
            current_sku_variation = None
            variations = item_container.get('variations')
            if isinstance(variations, list) and len(variations) > 0 and isinstance(variations[0], dict):
                current_sku_variation = str(variations[0].get('id', ''))

            product_name_found = item_container.get('display_name', 'N/A')

            if current_sku_primary == sku_id_to_find_str or \
               (current_sku_variation and current_sku_variation == sku_id_to_find_str):
                
                is_in_stock = item_container.get('in_stock', False)
                is_available = item_container.get('available', False)
                
                # Check variation stock if main item shows OOS
                variation_in_stock = False
                if variations and isinstance(variations[0], dict) and isinstance(variations[0].get('inventory'), dict):
                    variation_in_stock = variations[0]['inventory'].get('in_stock', False)

                if not (is_in_stock and is_available) and not variation_in_stock:
                    print(f"  Found SKU {sku_id_to_find} ('{product_name_found}') but Out Of Stock (main and variation).")
                    return "OOS", product_name_found
                
                rank = index + 1
                print(f"  Found SKU {sku_id_to_find} ('{product_name_found}') at rank {rank}.")
                return rank, product_name_found
        
        print(f"  SKU {sku_id_to_find} not found in {len(product_items)} results.")
        return "Not Found", None

    except Exception as e:
        print(f"  Unexpected error parsing API response for SKU {sku_id_to_find}: {e}")
        import traceback
        traceback.print_exc()
    return "Parse Error (exception)", None


def main():
    worksheet = get_google_sheet_worksheet()
    if not worksheet: return

    # Use a session object to persist cookies and connection settings
    with curl_requests.Session() as session:
        try:
            store_id_cell_val = worksheet.cell(LOCATION_ROW, LOCATION_COL).value
            if not store_id_cell_val:
                print(f"ERROR: Store ID not found in cell {gspread.utils.rowcol_to_a1(LOCATION_ROW, LOCATION_COL)}.")
                return
            store_id = str(store_id_cell_val).strip()
            print(f"Using Store ID: {store_id} from sheet.")

            current_timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
            worksheet.update_cell(HEADER_ROW, RANK_COL, current_timestamp)
            print(f"Updated rank column header (Cell {gspread.utils.rowcol_to_a1(HEADER_ROW, RANK_COL)}) to: {current_timestamp}")

            all_sheet_data = worksheet.get_all_values() # Fetch all data once
            if len(all_sheet_data) < DATA_START_ROW:
                print(f"No data found starting from row {DATA_START_ROW}.")
                return
            
            # Prepare batch update
            updates_batch = []

            data_rows = all_sheet_data[DATA_START_ROW-1:] # Python slicing is 0-indexed

            for row_idx_in_slice, row_values in enumerate(data_rows):
                row_num_actual = DATA_START_ROW + row_idx_in_slice # Actual row number in sheet

                if len(row_values) < SKU_ID_COL: # Ensure enough columns exist
                    if not any(cell.strip() for cell in row_values[:SKU_ID_COL]): # If all relevant cells are empty
                        print(f"Skipping empty or incomplete row {row_num_actual}.")
                        continue
                    else:
                        print(f"Skipping row {row_num_actual}: not enough columns for Keyword/SKU.")
                        updates_batch.append({
                            'range': gspread.utils.rowcol_to_a1(row_num_actual, RANK_COL),
                            'values': [["Input Error (Cols)"]]
                        })
                        continue

                keyword = row_values[KEYWORD_COL - 1].strip() if len(row_values) >= KEYWORD_COL and row_values[KEYWORD_COL - 1] else None
                sku_id = row_values[SKU_ID_COL - 1].strip() if len(row_values) >= SKU_ID_COL and row_values[SKU_ID_COL - 1] else None

                if not keyword or not sku_id:
                    # Check if the entire row is empty (common for end of data)
                    if all(not cell.strip() for cell in row_values):
                        print(f"Reached a completely empty row ({row_num_actual}). Assuming end of data.")
                        break 
                    else: # Row has some data but keyword or SKU is missing
                        print(f"Warning: Missing keyword or SKU ID in row {row_num_actual}. Keyword: '{keyword}', SKU: '{sku_id}'. Skipping.")
                        updates_batch.append({
                            'range': gspread.utils.rowcol_to_a1(row_num_actual, RANK_COL),
                            'values': [["Input Error (Missing)"]]
                        })
                    continue
                
                api_response = fetch_search_results(session, keyword, store_id)
                
                rank_output = "API Error/No Data" # Default if api_response is None
                if api_response:
                    rank_output, _ = find_sku_rank(api_response, sku_id)
                
                updates_batch.append({
                    'range': gspread.utils.rowcol_to_a1(row_num_actual, RANK_COL),
                    'values': [[str(rank_output)]]
                })
                print(f"Prepared update for row {row_num_actual}: Rank/Status='{rank_output}'")

                # Apply batch update periodically or at the end to reduce API calls to Google Sheets
                if len(updates_batch) >= 10: # Update every 10 rows, for example
                    try:
                        worksheet.batch_update(updates_batch)
                        print(f"Batch updated {len(updates_batch)} rows in Google Sheet.")
                        updates_batch = [] # Clear batch
                    except gspread.exceptions.APIError as e_sheet_batch:
                        print(f"Google Sheets API Error during batch update: {e_sheet_batch}. Will retry individual updates for this batch.")
                        # Fallback to individual updates if batch fails
                        for item in updates_batch:
                            try:
                                worksheet.update_acell(item['range'], item['values'][0][0])
                            except Exception as e_sheet_single:
                                print(f"Error updating cell {item['range']} individually: {e_sheet_single}")
                        updates_batch = [] # Clear batch
                    except Exception as e_sheet_generic_batch:
                        print(f"Unexpected error during batch update: {e_sheet_generic_batch}")
                        updates_batch = [] # Clear batch to prevent reprocessing same failed items


                delay = random.uniform(MIN_DELAY_SECONDS, MAX_DELAY_SECONDS)
                print(f"Waiting for {delay:.2f} seconds before next keyword...")
                time.sleep(delay)
            
            # Update any remaining items in the batch
            if updates_batch:
                try:
                    worksheet.batch_update(updates_batch)
                    print(f"Final batch updated {len(updates_batch)} rows in Google Sheet.")
                except Exception as e_sheet_final:
                    print(f"Google Sheets API Error during final batch update: {e_sheet_final}")
                    for item in updates_batch: # Fallback for final batch
                        try: worksheet.update_acell(item['range'], item['values'][0][0])
                        except Exception as e_sf: print(f"Error updating cell {item['range']} individually: {e_sf}")


            print("\n--- Processing Complete ---")

        except gspread.exceptions.SpreadsheetNotFound: print(f"Error: Spreadsheet '{GOOGLE_SHEET_NAME}' not found.")
        except gspread.exceptions.WorksheetNotFound: print(f"Error: Worksheet '{WORKSHEET_NAME}' not found.")
        except gspread.exceptions.APIError as e_gspread:
            print(f"A Google Sheets API error occurred: {e_gspread}")
        except Exception as e:
            print(f"An critical error occurred in the main process: {e}")
            import traceback
            traceback.print_exc()

if __name__ == "__main__":
    if "path/to/your/credentials.json" in GOOGLE_CREDENTIALS_FILE :
        print("ERROR: Please update 'GOOGLE_CREDENTIALS_FILE' in the script with the correct path.")
    elif not GOOGLE_SHEET_NAME or GOOGLE_SHEET_NAME == "Your Google Sheet Name":
        print("ERROR: Please update 'GOOGLE_SHEET_NAME' in the script.")
    else:
        main()
