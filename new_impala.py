import json
import ssl
import sys
import base64
import logging
from datetime import datetime, timedelta
import urllib.request
from urllib.error import URLError, HTTPError

# ==========================================
# CONFIGURATION
# ==========================================
# CM Details
CM_HOST = 'https://your-cm-host.com'
CM_PORT = '7183'
CM_USER = 'admin'
CM_PASS = 'password'

# Cluster Details
CLUSTER_NAME = 'Cluster 1'  # Must match exactly as seen in CM
SERVICE_NAME = 'impala'     # Usually 'impala', sometimes 'impala-1'

# Analysis Settings
LOOKBACK_HOURS = 24
MIN_DURATION_SECONDS = 5.0  # Ignore fast queries to speed up analysis
MAX_QUERIES_TO_FETCH = 5000 # Safety limit to prevent memory issues on large fetches

# ==========================================
# UTILITIES & SETUP
# ==========================================
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Disable SSL verification (common for internal CM hosts with self-signed certs)
ctx = ssl.create_default_context()
ctx.check_hostname = False
ctx.verify_mode = ssl.CERT_NONE

def get_auth_headers(username, password):
    auth_str = f'{username}:{password}'
    b64_auth = base64.b64encode(auth_str.encode()).decode()
    return {'Authorization': f'Basic {b64_auth}'}

def make_request(url):
    """Executes a GET request and returns parsed JSON."""
    req = urllib.request.Request(url, headers=get_auth_headers(CM_USER, CM_PASS))
    try:
        with urllib.request.urlopen(req, context=ctx) as response:
            return json.loads(response.read().decode())
    except HTTPError as e:
        logging.error(f"HTTP Error {e.code}: {e.reason} - URL: {url}")
        if e.code == 401:
            logging.error("Authentication failed. Check CM_USER and CM_PASS.")
        elif e.code == 404:
            logging.error("Resource not found. Check CLUSTER_NAME and SERVICE_NAME.")
        sys.exit(1)
    except URLError as e:
        logging.error(f"URL Error: {e.reason} - Check host reachability.")
        sys.exit(1)

def print_table(headers, rows, col_widths):
    """A manual implementation of a table printer to avoid 'tabulate' dependency."""
    # Print Header
    header_str = " | ".join(f"{h:<{w}}" for h, w in zip(headers, col_widths))
    print("-" * len(header_str))
    print(header_str)
    print("-" * len(header_str))
    
    # Print Rows
    for row in rows:
        row_str = " | ".join(f"{str(item):<{w}}" for item, w in zip(row, col_widths))
        print(row_str)

# ==========================================
# CORE LOGIC
# ==========================================

def fetch_queries():
    end_time = datetime.utcnow()
    start_time = end_time - timedelta(hours=LOOKBACK_HOURS)
    
    # ISO 8601 Formatting for CM API
    from_str = start_time.strftime('%Y-%m-%dT%H:%M:%S.000Z')
    to_str = end_time.strftime('%Y-%m-%dT%H:%M:%S.000Z')
    
    # Filter syntax for CM API
    filter_str = f'queryDuration > {MIN_DURATION_SECONDS}s'
    
    # Encode parameters safely
    params = urllib.parse.urlencode({
        'from': from_str,
        'to': to_str,
        'filter': filter_str,
        'limit': 1000,
        'offset': 0
    })

    base_url = f"{CM_HOST}:{CM_PORT}/api/v43/clusters/{CLUSTER_NAME}/services/{SERVICE_NAME}/impalaQueries"
    
    all_queries = []
    offset = 0
    
    logging.info(f"Fetching queries > {MIN_DURATION_SECONDS}s from last {LOOKBACK_HOURS}h...")

    while True:
        # Re-build query string with current offset
        current_params = urllib.parse.urlencode({
            'from': from_str,
            'to': to_str,
            'filter': filter_str,
            'limit': 1000,
            'offset': offset
        })
        
        url = f"{base_url}?{current_params}"
        data = make_request(url)
        
        queries = data.get('queries', [])
        if not queries:
            break
            
        all_queries.extend(queries)
        offset += len(queries)
        
        logging.info(f"Fetched {len(all_queries)} queries...")
        
        if len(queries) < 1000 or len(all_queries) >= MAX_QUERIES_TO_FETCH:
            break
            
    return all_queries

def analyze_and_report(queries):
    if not queries:
        print("\nNo queries found matching the criteria.")
        return

    # Metrics containers
    total_count = len(queries)
    spilled_count = 0
    missing_stats_count = 0
    high_wait_count = 0
    
    # For Top N lists
    slowest_queries = []
    user_stats = {}

    for q in queries:
        attrs = q.get('attributes', {})
        
        # safely get attributes
        duration_ms = attrs.get('query_duration', 0)
        duration_s = float(duration_ms) / 1000.0
        user = q.get('user', 'unknown')
        query_id = q.get('queryId', 'N/A')
        
        # Check Booleans (CM returns them as strings 'true'/'false' or actual booleans depending on version)
        is_spilled = str(attrs.get('spilled', 'false')).lower() == 'true'
        is_missing_stats = str(attrs.get('stats_missing', 'false')).lower() == 'true'
        
        wait_ms = float(attrs.get('admission_wait', 0))
        
        # 1. Aggregate Bottlenecks
        if is_spilled:
            spilled_count += 1
        if is_missing_stats:
            missing_stats_count += 1
        if wait_ms > 1000: # Wait > 1 second
            high_wait_count += 1
            
        # 2. Collect for Top N
        slowest_queries.append({
            'user': user,
            'duration': duration_s,
            'spilled': "YES" if is_spilled else "No",
            'stats': "MISSING" if is_missing_stats else "Ok",
            'id': query_id
        })
        
        # 3. User Aggregation
        if user not in user_stats:
            user_stats[user] = {'count': 0, 'total_duration': 0.0}
        user_stats[user]['count'] += 1
        user_stats[user]['total_duration'] += duration_s

    # --- GENERATE REPORT ---
    print("\n" + "="*60)
    print(f" IMPALA HEALTH REPORT (Last {LOOKBACK_HOURS} Hours)")
    print("="*60)
    print(f"Total Analyzed: {total_count}")
    
    # 1. Bottleneck Overview
    spill_pct = (spilled_count / total_count) * 100
    stats_pct = (missing_stats_count / total_count) * 100
    wait_pct = (high_wait_count / total_count) * 100
    
    print("\n>>> 1. BOTTLENECK DISTRIBUTION")
    overview_headers = ["Issue", "Count", "% Workload", "Action"]
    overview_widths = [25, 8, 12, 30]
    overview_data = [
        ["Spilling to Disk", spilled_count, f"{spill_pct:.1f}%", "Increase Mem / Fix Joins"],
        ["Missing Table Stats", missing_stats_count, f"{stats_pct:.1f}%", "Run COMPUTE STATS"],
        ["Admission Queueing", high_wait_count, f"{wait_pct:.1f}%", "Check Resource Pools"]
    ]
    print_table(overview_headers, overview_data, overview_widths)

    # 2. Top Slow Queries
    print("\n>>> 2. TOP 5 SLOWEST QUERIES")
    # Sort by duration descending
    slowest_queries.sort(key=lambda x: x['duration'], reverse=True)
    
    top_headers = ["User", "Dur(s)", "Spilled?", "Stats?", "Query ID"]
    top_widths = [15, 10, 10, 10, 35]
    top_data = []
    for q in slowest_queries[:5]:
        top_data.append([q['user'], f"{q['duration']:.1f}", q['spilled'], q['stats'], q['id']])
    
    print_table(top_headers, top_data, top_widths)

    # 3. Top Heavy Users
    print("\n>>> 3. TOP USERS (By Resource Time)")
    # Convert dict to list for sorting
    user_list = []
    for u, stats in user_stats.items():
        user_list.append([u, stats['count'], stats['total_duration']])
    
    # Sort by total duration
    user_list.sort(key=lambda x: x[2], reverse=True)
    
    user_headers = ["User", "Query Count", "Total Duration(s)"]
    user_widths = [20, 15, 20]
    user_data = []
    for u in user_list[:5]:
        user_data.append([u[0], u[1], f"{u[2]:.1f}"])
        
    print_table(user_headers, user_data, user_widths)
    print("\n")

if __name__ == "__main__":
    try:
        data = fetch_queries()
        analyze_and_report(data)
    except KeyboardInterrupt:
        print("\nAnalysis cancelled by user.")
    except Exception as e:
        logging.error(f"Unexpected error: {e}")
