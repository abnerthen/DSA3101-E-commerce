from google.cloud import bigquery

def load_data():
    client = bigquery.Client()
    query = """
    SELECT 
      visitNumber, visitId, visitStartTime, date, 
      totals, trafficSource, device, geoNetwork, customDimensions, hits, 
      fullVisitorId, channelGrouping, socialEngagementType
    FROM 
      `bigquery-public-data.google_analytics_sample.ga_sessions_*`
    WHERE 
      _TABLE_SUFFIX BETWEEN '20160801' AND '20170801';
    """
    query_job = client.query(query)
    results = query_job.result()

    for row in results:
        print(row)

if __name__ == "__main__":
    load_data()
