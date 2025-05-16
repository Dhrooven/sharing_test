import time
import random
import json
from curl_cffi import requests as curl_requests # Using curl_cffi

# --- CONFIGURATION ---
# Swiggy API Configuration
API_BASE_URL = "https://www.swiggy.com/api/instamart/search"
DEFAULT_PARAMS = { # These will be URL parameters
    "pageNumber": "0",
    "searchResultsOffset": "0",
    "limit": "40",
    "ageConsent": "false",
    "layoutId": "2671",
    "pageType": "INSTAMART_PRE_SEARCH_PAGE",
    "isPreSearchTag": "false",
    "highConfidencePageNo": "0",
    "lowConfidencePageNo": "0",
    "voiceSearchTrackingId": "",
    # storeId, primaryStoreId, secondaryStoreId will be added from logic
}

# Anti-bot settings
MIN_DELAY_SECONDS = 10
MAX_DELAY_SECONDS = 28
# Updated to match browser log (Chrome 136)
IMPERSONATE_BROWSER = "chrome116"

# --- END CONFIGURATION ---

def get_request_headers(keyword, store_id_for_header=None):
    """
    Returns a dictionary of headers, attempting to mimic a browser.
    aws-waf-token and matcher are commented out as they are likely causing WAF blocks.
    """
    # Specific User-Agent from the successful browser log
    specific_user_agent = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36"

    headers = {
        "User-Agent": specific_user_agent,
        "Accept": "*/*",
        "Accept-Language": "en-GB,en-US;q=0.9,en;q=0.8",
        "Accept-Encoding": "gzip, deflate, br, zstd", # curl_cffi handles actual encoding
        "Content-Type": "application/json",
        "Origin": "https://www.swiggy.com",
        # Dynamic Referer based on the search query
        "Referer": f"https://www.swiggy.com/instamart/search?query={keyword}",
        "DNT": "1", # Do Not Track
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-origin",
        "x-build-version": "2.273.0", # From user's browser log
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
        # "matcher": "dd7dc8ebeb97ecafedgcdf7", # Commented out: HIGHLY LIKELY DYNAMIC & CAUSING WAF BLOCK
        # "aws-waf-token": "ac4eabe1-2c74-4b11-af7d-a7e7eb9cf7e6:...", # Commented out: EXTREMELY DYNAMIC & CAUSING WAF BLOCK
        "sec-ch-ua": '"Chromium";v="136", "Google Chrome";v="136", "Not.A/Brand";v="99"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"Linux"',
        "priority": "u=1, i",
    }
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
    request_headers = get_request_headers(keyword, store_id_from_sheet)

    print(f"\n--- Request for Keyword: '{keyword}', Store ID: '{store_id_from_sheet}' ---")
    print(f"URL: {api_url}")
    print(f"URL Params: {json.dumps(url_params)}")
    print(f"JSON Body: {json.dumps(post_data_payload)}")
    print(f"Request Headers (selected):")
    print(f"  User-Agent: {request_headers.get('User-Agent')}")
    print(f"  Referer: {request_headers.get('Referer')}")
    # print(f"Request Headers (Full): {json.dumps(request_headers, indent=2)}") # Uncomment for full header debug

    try:
        response = session.post(
            api_url,
            params=url_params,
            json=post_data_payload,
            headers=request_headers,
            timeout=30,
            impersonate=IMPERSONATE_BROWSER
        )
        
        print("\n--- Response Details ---")
        print(f"Status Code: {response.status_code}")
        print(f"Response Content-Type: {response.headers.get('Content-Type', 'None')}")
        print(f"Response Content-Length: {response.headers.get('Content-Length', 'N/A')}")

        print("\nSession Cookies After Request:")
        if session.cookies:
            for cookie_name, cookie_value in session.cookies.items():
                print(f"  {cookie_name}: {cookie_value}")
        else:
            print("  No cookies in session.")

        if response.status_code == 200 and response.headers.get('Content-Length') == '0':
            print("Warning: Received 200 OK with Content-Length 0. This might indicate a WAF block.")
            print(f"  X-Cache: {response.headers.get('x-cache')}")
            print(f"  Via: {response.headers.get('via')}")


        content_type = response.headers.get("Content-Type", "").lower()
        if "application/json" not in content_type:
            print(f"\nWarning: Response for '{keyword}' is NOT JSON. Actual Content-Type: '{response.headers.get('Content-Type', 'None')}'")
            print(f"Response Text (first 500 chars):\n{response.text[:500]}")
            if response.headers.get('Content-Length') == '0' or not response.text.strip():
                 print("Empty response body received.")
                 return None
        
        response.raise_for_status() 
        
        if response.text.strip():
            print("\n--- Raw JSON Response ---")
            try:
                parsed_json = response.json()
                print(json.dumps(parsed_json, indent=2))
                return parsed_json
            except json.JSONDecodeError:
                print("Could not decode JSON, printing raw text:")
                print(response.text)
                return None
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
    except Exception as e:
        print(f"\nAn unexpected error occurred while fetching data for '{keyword}': {e}")
        import traceback
        traceback.print_exc()
    return None


def main():
    """Main function to test fetching Swiggy API data."""
    # --- Test Parameters ---
    test_keyword = "bread"
    test_store_id = "1396284"
    # --- End Test Parameters ---

    with curl_requests.Session() as session:
        print(f"Attempting to fetch data for keyword: '{test_keyword}', Store ID: '{test_store_id}'")
        
        api_response = fetch_search_results(session, test_keyword, test_store_id)
        
        if api_response:
            print(f"\nSuccessfully fetched and printed API response for '{test_keyword}'.")
        else:
            print(f"\nFailed to fetch or parse API response for '{test_keyword}'.")
            
    print("\n--- Script Execution Complete ---")


if __name__ == "__main__":
    main()
