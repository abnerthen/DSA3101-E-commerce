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
        date,
        SUM(totals.totalTransactionRevenue / 1e6) AS total_revenue
      FROM
        `bigquery-public-data.google_analytics_sample.ga_sessions_*`
      WHERE
        _TABLE_SUFFIX BETWEEN '20160801' AND '20170801'
      GROUP BY
        date
    '''
    total_revenue = client.query(query).result().to_dataframe()
    total_revenue['date'] = pd.to_datetime(total_revenue['date'])

    query = '''
      SELECT
        date,
        product.v2ProductCategory AS product_category,
        COUNT(DISTINCT CONCAT(fullVisitorId, CAST(visitId AS STRING))) AS transaction_count,
        SUM(IFNULL(product.productRevenue / 1e6, 0)) AS revenue
      FROM
        `bigquery-public-data.google_analytics_sample.ga_sessions_*`,
        UNNEST(hits) AS hits,
        UNNEST(hits.product) AS product
      WHERE
        _TABLE_SUFFIX BETWEEN '20160801' AND '20170801'
      GROUP BY
        date, product_category
    '''
    agg_sales_by_cat = client.query(query).result().to_dataframe()
    agg_sales_by_cat['date'] = pd.to_datetime(agg_sales_by_cat['date']).dt.strftime('%Y-%m')
    agg_sales_by_cat = cat_cleaning.clean_categories(agg_sales_by_cat, 'product_category')
    agg_sales_by_cat = agg_sales_by_cat.groupby(
        ['date', 'main_category', 'sub_category', 'subsub_category']).agg(
            {'transaction_count': 'sum',
             'revenue': 'sum'}
        ).reset_index()
    agg_sales_by_cat['date'] = pd.to_datetime(agg_sales_by_cat['date'])


