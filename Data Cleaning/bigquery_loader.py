from google.cloud import bigquery
from google.oauth2 import service_account
import pandas as pd
import cat_cleaning
from dotenv import load_dotenv
import os
import pyarrow.parquet as pq

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
    total_revenue.to_parquet('total_revenue.parquet')

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
    agg_sales_by_cat.to_parquet('agg_sales_by_cat.parquet')

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

    purchase_averages = client.query(query).result().to_arrow()
    pq.write_table(purchase_averages, 'purchase_averages.parquet')

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
    top_customers = client.query(query).result().to_arrow()
    pq.write_table(top_customers, 'top_customers.parquet')

    query = '''
    SELECT
      visitNumber AS visit_number,
      totals.timeOnSite AS time_on_site,
      totals.pageviews AS pageviews,
      totals.hits AS num_hits,
      totals.sessionQualityDim AS session_quality,
      totals.totalTransactionRevenue / 1e6 AS totals_revenue,
      totals.transactions AS transactions
    FROM
      `bigquery-public-data.google_analytics_sample.ga_sessions_*`
      WHERE _TABLE_SUFFIX BETWEEN '20160801' AND '20170801'
    '''

    numerical_correlation = client.query(query).result().to_dataframe()
    numerical_correlation['totals_revenue'] = numerical_correlation['totals_revenue'].fillna(0)
    numerical_correlation['session_quality'] = numerical_correlation['session_quality'].fillna(-1)
    numerical_correlation['transactions'] = numerical_correlation['transactions'].fillna(0)
    numerical_correlation.to_parquet('numerical_correlation.parquet')

    query = '''
    SELECT
    fullVisitorId AS users,
    hits.eCommerceAction.action_type AS action,
    product.productPrice / 1e6 AS price,
    product.isImpression AS impression,
    product.isClick AS click
    FROM
    `bigquery-public-data.google_analytics_sample.ga_sessions_*`,
    UNNEST(hits) AS hits,
    UNNEST(hits.product) AS product
    WHERE
    _TABLE_SUFFIX BETWEEN '20160801' AND '20170801'
    '''

    nested_correlation = client.query(query).result().to_dataframe()
    nested_correlation['action'] = nested_correlation['action'].apply(int)
    nested_correlation['click'] = nested_correlation['click'].apply(lambda x: 1 if x is True else 0)
    nested_correlation['impression'] = nested_correlation['impression'].apply(lambda x: 1 if x is True else 0)
    nested_correlation.to_parquet('nested_correlation.parquet')

    query = '''
    SELECT
    *,
    CASE
        WHEN avg_order_value < 10 THEN 'Price-Sensitive'
        WHEN avg_order_value BETWEEN 10 AND 20 THEN 'Semi Price-Sensitive'
        WHEN avg_order_value BETWEEN 20 AND 50 THEN 'Medium Price-Sensitive'
        WHEN avg_order_value BETWEEN 50 AND 100 THEN 'Semi-Low Price-Sensitive'
        ELSE 'Low-Sensitivity'
        END AS price_sensitivity_segment
    FROM (
    SELECT
        fullVisitorId,
        COUNT(DISTINCT hits.transaction.transactionId) AS purchase_count,
        AVG(hits.transaction.transactionRevenue / 1000000) AS avg_order_value,

    FROM
        `bigquery-public-data.google_analytics_sample.ga_sessions_*`,
        UNNEST(hits) AS hits
    WHERE
        _TABLE_SUFFIX BETWEEN '20160801' AND '20170801'
    GROUP BY
        fullVisitorId
    )
    '''

    price_sensitive = client.query(query).result().to_arrow()
    pq.write_table(price_sensitive, 'price_sensitive.parquet')

    query = '''
    SELECT
    geoNetwork.continent AS continent,
    geoNetwork.subContinent AS subcontinent,
    geoNetwork.country AS country,
    SUM(totals.totalTransactionRevenue / 1e6) AS total_revenue,
    SUM(totals.transactions) AS total_transactions
    FROM
    `bigquery-public-data.google_analytics_sample.ga_sessions_*`
    WHERE
    _TABLE_SUFFIX BETWEEN '20160801' AND '20170801'
    GROUP BY
    continent, subcontinent, country
    '''

    by_region = client.query(query).result().to_arrow()
    pq.write_table(by_region, 'by_region.parquet')

    query = '''
    SELECT
        product.v2ProductCategory AS product_category,
        COUNT(DISTINCT CONCAT(fullVisitorId, CAST(visitId AS STRING))) AS transaction_count,
        SUM(IFNULL(product.productRevenue / 1e6, 0)) AS revenue
    FROM
        `bigquery-public-data.google_analytics_sample.ga_sessions_*`,
        UNNEST(hits) AS hits,
        UNNEST(hits.product) AS product
    WHERE
        _TABLE_SUFFIX BETWEEN '20160801' AND '20170801'
    AND
        totals.transactions IS NOT NULL
    GROUP BY
        product_category
    ORDER BY
        transaction_count DESC, revenue DESC;
    '''

    by_category = client.query(query).result().to_dataframe()
    by_category = cat_cleaning.clean_categories(by_category, 'product_category')
    by_category.to_parquet('by_category.parquet')

  
