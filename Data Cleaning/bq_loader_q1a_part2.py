from google.cloud import bigquery
from google.oauth2 import service_account
import pandas as pd
import cat_cleaning
from dotenv import load_dotenv
import os

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
      fullVisitorId AS users,
      COUNT(DISTINCT hits.transaction.transactionId) AS total_purchases,
      SUM(IFNULL(hits.transaction.transactionRevenue / 1e6, 0)) AS total_revenue
    FROM
        `bigquery-public-data.google_analytics_sample.ga_sessions_*`,
        UNNEST(hits) AS hits
    WHERE
        _TABLE_SUFFIX BETWEEN '20160801' AND '20170801'
      AND
        hits.eCommerceAction.action_type = '6'
    GROUP BY
      fullVisitorId
    '''
    purchases_per_user = client.query(query).result().to_dataframe()

    query = '''
    WITH visitsWithPurchases AS (
      SELECT
        fullVisitorId AS users,
        visitId
      FROM
        `bigquery-public-data.google_analytics_sample.ga_sessions_*`,
        UNNEST(hits) AS hits
      WHERE
        _TABLE_SUFFIX BETWEEN '20160801' AND '20170801'
      AND
        hits.eCommerceAction.action_type = '6'
    )

    SELECT
        CASE
            WHEN p.visitId IS NOT NULL THEN 'Purchase'
            ELSE 'No Purchase'
        END AS visit_type,
        AVG(visitNumber) AS average_visit_no,
        AVG(totals.timeOnSite) AS avg_time,
        AVG(totals.pageviews) AS avg_pgviews
    FROM
        `bigquery-public-data.google_analytics_sample.ga_sessions_*` v
    LEFT JOIN
        visitsWithPurchases p
    ON
        v.fullVisitorId = p.users
        AND v.visitId = p.visitId
    WHERE _TABLE_SUFFIX BETWEEN '20160801' AND '20170801'
    GROUP BY visit_type
    '''
    purchase_averages = client.query(query).result().to_dataframe()

    query = '''
    SELECT
      fullVisitorId AS users,
      SUM(IFNULL(totals.pageviews, 0)) AS pageviews,
      SUM(IFNULL(totals.timeOnSite, 0)) AS timeOnSite,
      SUM(totals.transactions) AS transactions
    FROM
      `bigquery-public-data.google_analytics_sample.ga_sessions_*`
    WHERE
      _TABLE_SUFFIX BETWEEN '20160801' AND '20170801'
    GROUP BY
      fullVisitorId
    ORDER BY
      transactions DESC,
      timeOnSite DESC,
      pageviews DESC
    '''
    top_customers = client.query(query).result().to_dataframe()

    