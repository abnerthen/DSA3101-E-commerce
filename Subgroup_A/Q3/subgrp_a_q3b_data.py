from google.cloud import bigquery
from google.oauth2 import service_account
import pandas as pd
from dotenv import load_dotenv
import os
import pyarrow.parquet as pq
import time

load_dotenv()

key = {
  "type": "service_account",
  "project_id": os.getenv("PROJ_ID"),
  "private_key_id": os.getenv("PKEY_ID"),
  "private_key": os.getenv("GCP_PKEY"),
  "client_email": os.getenv("CLIENT_EMAIL"),
  "client_id": "106977152441456710656",
  "auth_uri": "https://accounts.google.com/o/oauth2/auth",
  "token_uri": "https://oauth2.googleapis.com/token",
  "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
  "client_x509_cert_url": os.getenv("client_x509_cert_url"),
  "universe_domain": "googleapis.com"
}

credentials = service_account.Credentials.from_service_account_info(key)
client = bigquery.Client(credentials= credentials,project=os.getenv("PROJ_ID"))

if __name__ == "__main__":
    query = '''
    SELECT
        *,
        total_revenue / total_transactions AS avg_order_value,
        total_transactions / total_sessions AS conversion_rate
    FROM (
        SELECT
            trafficSource.medium AS channel,
            COUNT(DISTINCT fullVisitorId) AS total_users,
            COUNT(*) AS total_sessions,
            SUM(totals.transactions) AS total_transactions,
            SUM(totals.transactionRevenue) / 1e6 AS total_revenue -- converting from micros
            , AVG(totals.pageviews) AS avg_pageviews,
            AVG(totals.timeOnSite) AS avg_time_on_site,
            AVG(IF(totals.bounces IS NOT NULL, totals.bounces, 0)) AS avg_bounces
        FROM
            `bigquery-public-data.google_analytics_sample.ga_sessions_*`
        WHERE
            _TABLE_SUFFIX BETWEEN '20160801' AND '20170801' -- example date range
        GROUP BY
            trafficSource.medium
        ORDER BY
            total_revenue DESC
        );
    '''
    result = client.query(query).result().to_arrow()
    pq.write_table(result, 'aov_conversion.parquet')

    query = '''
    SELECT
    *,
    IF(total_transactions IS NOT NULL, total_revenue / total_transactions, 0) AS avg_order_value,
    total_transactions / total_sessions AS conversion_rate
    FROM (
    SELECT
        trafficSource.campaign,
        COUNT(DISTINCT fullVisitorId) AS total_users,
        COUNT(*) AS total_sessions,
        IF(SUM(totals.transactions) IS NOT NULL, SUM(totals.transactions), 0) AS total_transactions,
        SUM(totals.transactionRevenue) / 1e6 AS total_revenue,
        AVG(IF(totals.pageviews IS NOT NULL, totals.pageviews, 0)) AS avg_pageviews,
        AVG(IF(totals.timeOnSite IS NOT NULL, totals.timeOnSite, 0)) AS avg_time_on_site,
        AVG(IF(totals.bounces IS NOT NULL, totals.bounces, 0)) AS avg_bounces
    FROM
        `bigquery-public-data.google_analytics_sample.ga_sessions_*`
    WHERE
        _TABLE_SUFFIX BETWEEN '20160801' AND '20170801' -- example date range
    GROUP BY
        trafficSource.campaign
    ORDER BY
        total_revenue DESC
    )
    ORDER BY
    avg_order_value DESC;
    '''

    result = client.query(query).result().to_arrow()
    pq.write_table(result, 'marketing_metrics.parquet')

    query = '''
    SELECT
        trafficSource.campaign,
        totals.pageviews AS pageviews,
        totals.timeOnSite AS time_on_site
    FROM
        `bigquery-public-data.google_analytics_sample.ga_sessions_*`
    WHERE
        totals.pageviews IS NOT NULL AND totals.timeOnSite IS NOT NULL
        AND _TABLE_SUFFIX BETWEEN '20160801' AND '20170801'
    '''

    result = client.query(query).result().to_arrow()
    pq.write_table(result, 'campaign_distribution.parquet')

    query = '''
    SELECT
        trafficSource.campaign,
        date,
        COUNTIF(totals.newVisits IS NULL) / COUNT(*) AS return_rate
    FROM
        `bigquery-public-data.google_analytics_sample.ga_sessions_*`
    WHERE
        _TABLE_SUFFIX BETWEEN '20160801' AND '20170801'
    GROUP BY
        trafficSource.campaign, date
    ORDER BY
        date
    '''

    res2 = client.query(query).result().to_dataframe()
    res2['date'] = pd.to_datetime(res2['date'])
    res2.to_parquet('return_rate_by_campaign.parquet')
