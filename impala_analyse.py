import cm_client
from cm_client.rest import ApiException
from datetime import datetime, timedelta
import pandas as pd
from tabulate import tabulate
import logging

# ==========================================
# CONFIGURATION
# ==========================================
CM_HOST = 'https://your-cm-host.com'
CM_PORT = '7183'  # Default TLS port
CM_USER = 'admin'  # Use a read-only user if available
CM_PASS = 'password'
CLUSTER_NAME = 'Cluster 1'
SERVICE_NAME = 'impala' 

# Thresholds for "Noise" filtering
MIN_DURATION_SECONDS = 5.0  # Only analyze queries longer than this
LOOKBACK_HOURS = 24         # Analyze last 24 hours

# ==========================================
# SETUP
# ==========================================
pd.set_option('display.max_columns', None)
pd.set_option('display.width', 1000)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def get_cm_client():
    cm_client.configuration.username = CM_USER
    cm_client.configuration.password = CM_PASS
    api_url = f"{CM_HOST}:{CM_PORT}/api/v43" # v43 is safe for CDP 7.1.x
    return cm_client.ApiClient(api_url)

def fetch_impala_queries(api_client):
    cluster_api = cm_client.ClustersResourceApi(api_client)
    impala_api = cm_client.ImpalaQueriesResourceApi(api_client)
    
    end_time = datetime.utcnow()
    start_time = end_time - timedelta(hours=LOOKBACK_HOURS)
    
    # Filter: Only queries > 5s to reduce API load and noise
    filter_str = f'queryDuration > {MIN_DURATION_SECONDS}s'
    
    logging.info(f"Fetching Impala queries from {start_time} to {end_time}...")
    
    all_queries = []
    offset = 0
    limit = 1000
    
    try:
        while True:
            # Fetch pages of queries
            resp = impala_api.get_impala_queries(
                cluster_name=CLUSTER_NAME,
                service_name=SERVICE_NAME,
                filter=filter_str,
                from_time=start_time,
                to_time=end_time,
                limit=limit,
                offset=offset
            )
            
            if not resp.queries:
                break
                
            for q in resp.queries:
                # flatten the attributes for easy DataFrame conversion
                q_data = {
                    'queryId': q.query_id,
                    'user': q.user,
                    'duration': q.attributes.get('query_duration', 0) / 1000.0, # Convert ms to s
                    'state': q.query_state,
                    'memory_per_node_peak': q.attributes.get('memory_per_node_peak', 0),
                    'spilled': str(q.attributes.get('spilled', 'false')).lower() == 'true',
                    'stats_missing': str(q.attributes.get('stats_missing', 'false')).lower() == 'true',
                    'admission_wait': q.attributes.get('admission_wait', 0) / 1000.0,
                    'rows_inserted': q.attributes.get('rows_inserted', 0),
                    'planning_wait_time': q.attributes.get('planning_wait_time', 0) / 1000.0,
                    'thread_network_receive_wait_time': q.attributes.get('thread_network_receive_wait_time', 0) / 1000.0
                }
                all_queries.append(q_data)
            
            offset += len(resp.queries)
            logging.info(f"Fetched {offset} queries so far...")
            
            if len(resp.queries) < limit:
                break
                
    except ApiException as e:
        logging.error(f"CM API Exception: {e}")
        return pd.DataFrame()

    return pd.DataFrame(all_queries)

def analyze_performance(df):
    if df.empty:
        print("No queries found matching criteria.")
        return

    print("\n" + "="*60)
    print(f"IMPALA PERFORMANCE REPORT (Last {LOOKBACK_HOURS}h)")
    print("="*60)
    print(f"Total Queries Analyzed (> {MIN_DURATION_SECONDS}s): {len(df)}")
    
    # 1. BOTTLENECK OVERVIEW
    spilled_count = df[df['spilled'] == True].shape[0]
    missing_stats_count = df[df['stats_missing'] == True].shape[0]
    high_wait_count = df[df['admission_wait'] > 1.0].shape[0] # Waiting > 1s
    
    overview = [
        ["Spilling to Disk (CRITICAL)", spilled_count, f"{(spilled_count/len(df))*100:.1f}%", "Increase MEM_LIMIT or optimize Join"],
        ["Missing Table Stats (MAJOR)", missing_stats_count, f"{(missing_stats_count/len(df))*100:.1f}%", "Run COMPUTE STATS"],
        ["Admission Queueing (>1s)", high_wait_count, f"{(high_wait_count/len(df))*100:.1f}%", "Cluster is busy or Pool limits too low"]
    ]
    
    print("\n>>> 1. BOTTLENECK DISTRIBUTION")
    print(tabulate(overview, headers=["Issue", "Count", "% of Workload", "Action"], tablefmt="simple"))

    # 2. TOP OFFENDERS (Longest Duration)
    print("\n>>> 2. TOP 5 SLOWEST QUERIES")
    top_slow = df.sort_values(by='duration', ascending=False).head(5)
    print(tabulate(top_slow[['user', 'duration', 'spilled', 'stats_missing', 'queryId']], headers='keys', tablefmt="simple", showindex=False))

    # 3. USER RESOURCE HOGS
    print("\n>>> 3. TOP USERS BY TOTAL DURATION")
    user_agg = df.groupby('user').agg(
        Total_Duration=('duration', 'sum'),
        Query_Count=('queryId', 'count'),
        Spill_Count=('spilled', 'sum')
    ).sort_values(by='Total_Duration', ascending=False).head(5)
    print(tabulate(user_agg, headers='keys', tablefmt="simple"))

    # 4. QUEUE WAIT ANALYSIS
    if df['admission_wait'].max() > 5:
        print("\n>>> 4. QUEUE CONGESTION ALERT")
        print(f"Max Wait Time: {df['admission_wait'].max()}s")
        print("Queries are waiting in the admission pool. Check your Dynamic Resource Pools configuration.")

if __name__ == "__main__":
    api_client = get_cm_client()
    df = fetch_impala_queries(api_client)
    analyze_performance(df)
