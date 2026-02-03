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
CM_HOST = 'https://your-cm-host.com'
CM_PORT = '7183'
CM_USER = 'admin'
CM_PASS = 'password'
CLUSTER_NAME = 'Cluster 1'
SERVICE_NAME = 'impala'

# Analysis Settings
LOOKBACK_HOURS = 24
# We lower the duration threshold to 0 because even fast queries 
# can get stuck in the queue, and we want to see ALL queuing behavior.
MIN_DURATION_SECONDS = 0.0 
MAX_QUERIES_TO_FETCH = 5000

# ==========================================
# SETUP (Same as before)
# ==========================================
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
ctx = ssl.create_default_context()
ctx.check_hostname = False
ctx.verify_mode = ssl.CERT_NONE

def get_auth_headers(username, password):
    auth_str = f'{username}:{password}'
    b64_auth = base64.b64encode(auth_str.encode()).decode()
    return {'Authorization': f'Basic {b64_auth}'}

def make_request(url):
    req = urllib.request.Request(url, headers=get_auth_headers(CM_USER, CM_PASS))
    try:
        with urllib.request.urlopen(req, context=ctx) as response:
            return json.loads(response.read().decode())
    except Exception as e:
        logging.error(f"Request failed: {e}")
        sys.exit(1)

def print_table(headers, rows, col_widths):
    header_str = " | ".join(f"{h:<{w}}" for h, w in zip(headers, col_widths))
    print("-" * len(header_str))
    print(header_str)
    print("-" * len(header_str))
    for row in rows:
        row_str = " | ".join(f"{str(item):<{w}}" for item, w in zip(row, col_widths))
        print(row_str)

# ==========================================
# LOGIC
# ==========================================
def fetch_queries():
    end_time = datetime.utcnow()
    start_time = end_time - timedelta(hours=LOOKBACK_HOURS)
    from_str = start_time.strftime('%Y-%m-%dT%H:%M:%S.000Z')
    to_str = end_time.strftime('%Y-%m-%dT%H:%M:%S.000Z')
    
    # Fetch queries that waited AT LEAST 100ms or took > 1s
    # This ensures we catch queuing issues even if execution was fast
    filter_str = 'admissionWait > 100 OR queryDuration > 1s'
    
    base_url = f"{CM_HOST}:{CM_PORT}/api/v43/clusters/{CLUSTER_NAME}/services/{SERVICE_NAME}/impalaQueries"
    
    all_queries = []
    offset = 0
    logging.info(f"Fetching traffic data from last {LOOKBACK_HOURS}h...")

    while True:
        params = urllib.parse.urlencode({
            'from': from_str, 'to': to_str, 'filter': filter_str,
            'limit': 1000, 'offset': offset
        })
        url = f"{base_url}?{params}"
        data = make_request(url)
        queries = data.get('queries', [])
        
        if not queries: break
        all_queries.extend(queries)
        offset += len(queries)
        
        if len(queries) < 1000 or len(all_queries) >= MAX_QUERIES_TO_FETCH: break
            
    return all_queries

def analyze_queuing(queries):
    if not queries:
        print("\nNo data found.")
        return

    # Data Structures for Aggregation
    coord_stats = {} # {hostname: {'total': 0, 'queued': 0, 'wait_time': 0}}
    pool_stats = {}  # {pool_name: {'total': 0, 'queued': 0}}

    for q in queries:
        attrs = q.get('attributes', {})
        
        # 1. Extract Coordinator Hostname
        # CM API structure for coordinator can vary, usually nested
        coord_node = q.get('coordinator', {})
        coord_host = coord_node.get('hostname') or coord_node.get('hostId') or "Unknown"
        
        # 2. Extract Resource Pool
        pool = attrs.get('request_pool') or attrs.get('pool') or "default"
        
        # 3. Extract Wait Time
        wait_ms = float(attrs.get('admission_wait', 0))
        is_queued = wait_ms > 1000 # Considered "queued" if waited > 1s
        
        # --- Aggregate by Coordinator ---
        if coord_host not in coord_stats:
            coord_stats[coord_host] = {'total': 0, 'queued': 0, 'wait_time': 0.0}
        
        coord_stats[coord_host]['total'] += 1
        coord_stats[coord_host]['wait_time'] += wait_ms
        if is_queued:
            coord_stats[coord_host]['queued'] += 1

        # --- Aggregate by Pool ---
        if pool not in pool_stats:
            pool_stats[pool] = {'total': 0, 'queued': 0}
        
        pool_stats[pool]['total'] += 1
        if is_queued:
            pool_stats[pool]['queued'] += 1

    # ================= REPORTING =================

    print("\n" + "="*70)
    print(f" IMPALA QUEUE ANALYSIS (Last {LOOKBACK_HOURS} Hours)")
    print("="*70)

    # REPORT 1: COORDINATOR LOAD BALANCING
    # This tells you if the Load Balancer is broken
    print("\n>>> 1. COORDINATOR DISTRIBUTION (Are requests skewed?)")
    coord_headers = ["Coordinator Host", "Total Qry", "Queued Qry", "% Queued", "Avg Wait(ms)"]
    coord_widths = [30, 10, 12, 10, 12]
    coord_rows = []
    
    for host, stats in coord_stats.items():
        pct_queued = (stats['queued'] / stats['total']) * 100 if stats['total'] > 0 else 0
        avg_wait = stats['wait_time'] / stats['total'] if stats['total'] > 0 else 0
        coord_rows.append([host, stats['total'], stats['queued'], f"{pct_queued:.1f}%", f"{avg_wait:.0f}"])
    
    # Sort by Total Queries to see where traffic is going
    coord_rows.sort(key=lambda x: x[1], reverse=True)
    print_table(coord_headers, coord_rows, coord_widths)

    # REPORT 2: RESOURCE POOL CONFIGURATION
    # This tells you if a specific pool is under-provisioned
    print("\n>>> 2. RESOURCE POOL PRESSURE (Which pool is hitting limits?)")
    pool_headers = ["Resource Pool", "Total Qry", "Queued Qry", "% Queued"]
    pool_widths = [25, 10, 12, 10]
    pool_rows = []

    for pool, stats in pool_stats.items():
        pct_queued = (stats['queued'] / stats['total']) * 100 if stats['total'] > 0 else 0
        pool_rows.append([pool, stats['total'], stats['queued'], f"{pct_queued:.1f}%"])
    
    # Sort by Queued Queries descending
    pool_rows.sort(key=lambda x: x[2], reverse=True)
    print_table(pool_headers, pool_rows, pool_widths)
    print("\n")

if __name__ == "__main__":
    queries = fetch_queries()
    analyze_queuing(queries)
