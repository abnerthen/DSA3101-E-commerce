from google.cloud import bigquery
from google.oauth2 import service_account
import pandas as pd
import cat_cleaning
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
    
    # if you wish to use all the files from this script
    '''
    total_revenue = pd.read_parquet('total_revenue.parquet')
    agg_sales_by_cat = pd.read_parquet('agg_sales_by_cat.parquet')
    purchases_per_user = pd.read_parquet('purchases_per_user.parquet')
    purchase_averages = pd.read_parquet('purchase_averages.parquet')
    top_customers = pd.read_parquet('top_customers.parquet')
    numerical_correlation = pd.read_parquet('numerical_correlation.parquet')
    nested_correlation = pd.read_parquet('nested_correlation.parquet')
    price_sensitive = pd.read_parquet('price_sensitive.parquet')
    by_region = pd.read_parquet('by_region.parquet')
    by_category = pd.read_parquet('by_category.parquet')
    '''


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
      ORDER BY
        date;
    '''
    start = time.time()
    total_revenue = client.query(query).result().to_dataframe()
    total_revenue['date'] = pd.to_datetime(total_revenue['date'])
    total_revenue.to_parquet('total_revenue.parquet')
    print(f'total_revenue.parquet ready, time taken: {time.time() - start}s')

    query = '''
    WITH checkedOutProducts AS (
      SELECT
        date,
        product.v2ProductCategory AS product_category,
        product.productQuantity AS qty,
        product.productRevenue / 1e6 AS revenue
      FROM
          `bigquery-public-data.google_analytics_sample.ga_sessions_*`,
          UNNEST(hits) AS hits,
          UNNEST(hits.product) AS product
      WHERE
          _TABLE_SUFFIX BETWEEN '20160801' AND '20170801'
        AND
          hits.eCommerceAction.action_type = '6'
    )
    SELECT
      date,
      product_category,
      SUM(IFNULL(qty,0)) AS sales_qty,
      SUM(IFNULL(revenue, 0)) AS revenue
    FROM
      checkedOutProducts
    GROUP BY
      date, product_category
    '''
    start = time.time()
    agg_sales_by_cat = client.query(query).result().to_dataframe()
    agg_sales_by_cat['date'] = pd.to_datetime(agg_sales_by_cat['date']).dt.strftime('%Y-%m')
    agg_sales_by_cat = cat_cleaning.clean_categories(agg_sales_by_cat, 'product_category')
    agg_sales = agg_sales_by_cat.groupby(
    ['date', 'main_category', 'sub_category', 'subsub_category']).agg({
        'sales_qty': ['sum', 'mean'],
        'revenue': ['sum', 'mean']
    }).reset_index()
    agg_sales['total_sales'] = agg_sales['sales_qty']['sum']
    agg_sales['total_revenue'] = agg_sales['revenue']['sum']
    agg_sales['avg_sales'] = agg_sales['sales_qty']['mean']
    agg_sales['avg_revenue'] = agg_sales['revenue']['mean']
    agg_sales.drop(['sales_qty', 'revenue'], axis = 1, inplace = True)
    agg_sales['date'] = pd.to_datetime(agg_sales_by_cat['date'])
    agg_sales.to_parquet('agg_sales_by_cat.parquet')
    print(f'agg_sales_by_cat.parquet ready, time taken: {time.time() - start}s')

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
    start = time.time()
    purchases_per_user = client.query(query).result().to_arrow()
    pq.write_table(purchases_per_user, 'purchases_per_user.parquet')
    print(f'purchases_per_user.parquet ready, time taken: {time.time() - start}s')

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
    start = time.time()
    purchase_averages = client.query(query).result().to_arrow()
    pq.write_table(purchase_averages, 'purchase_averages.parquet')
    print(f'purchase_averages.parquet ready, time taken: {time.time() - start}s')

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
    start = time.time()
    top_customers = client.query(query).result().to_arrow()
    pq.write_table(top_customers, 'top_customers.parquet')
    print(f'top_customers.parquet ready, time taken: {time.time() - start}s')

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
    start = time.time()
    numerical_correlation = client.query(query).result().to_dataframe()
    numerical_correlation['totals_revenue'] = numerical_correlation['totals_revenue'].fillna(0)
    numerical_correlation['session_quality'] = numerical_correlation['session_quality'].fillna(-1)
    numerical_correlation['transactions'] = numerical_correlation['transactions'].fillna(0)
    numerical_correlation.to_parquet('numerical_correlation.parquet')
    print(f'numerical_correlation.parquet ready, time taken: {time.time() - start}s')

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
    start = time.time()
    nested_correlation = client.query(query).result().to_dataframe()
    nested_correlation['action'] = nested_correlation['action'].apply(int)
    nested_correlation['click'] = nested_correlation['click'].apply(lambda x: 1 if x is True else 0)
    nested_correlation['impression'] = nested_correlation['impression'].apply(lambda x: 1 if x is True else 0)
    nested_correlation.to_parquet('nested_correlation.parquet')
    print(f'nested_correlation.parquet ready, time taken: {time.time() - start}s')

    query = """
    SELECT
        CASE WHEN hits.eCommerceAction.action_type = '1' THEN 'Click through of product lists'
              WHEN hits.eCommerceAction.action_type = '2' THEN 'Product detail views'
              WHEN hits.eCommerceAction.action_type = '5' THEN 'Check out'
              WHEN hits.eCommerceAction.action_type = '6' THEN 'Completed purchase'
        END AS action,
        CASE WHEN product.v2ProductCategory LIKE '%Office%'
          OR product.v2ProductCategory IN ('Notebooks & Journals', 'Writing') THEN 'Office'
        WHEN product.v2ProductCategory LIKE '%Apparel%'
          OR product.v2ProductCategory LIKE '%Wearables%'
          OR product.v2ProductCategory LIKE '%Men%' THEN 'Apparel'
        WHEN product.v2ProductCategory LIKE '%Bags%'
          OR product.v2ProductCategory IN ('Backpacks', 'More Bags') THEN 'Bags'
        WHEN product.v2ProductCategory LIKE '%Brand%'
          OR product.v2ProductCategory IN ('Apple', 'YouTube', 'Waze', 'Google') THEN 'Brands'
        WHEN product.v2ProductCategory LIKE '%Drinkware%'
          OR product.v2ProductCategory LIKE '%Fun%'
          OR product.v2ProductCategory IN ('Mugs', 'Fruit Games',
            'Housewares', 'Headgear', 'Headwear', 'Tumblers') THEN 'Accessories'
        WHEN product.v2ProductCategory LIKE '%Lifestyle%' THEN 'Lifestyle'
        WHEN product.v2ProductCategory LIKE '%Electronics%' THEN 'Electronics'
        WHEN product.v2ProductCategory IN ('Home', '${escCatTitle}', '(not set)',
        '${productitem.product.origCatName}')
           THEN 'Unavailable'
        WHEN product.v2ProductCategory LIKE '%Sale%' THEN 'Sale'
        ELSE 'Other'
        END AS category,
        COUNT(fullVisitorID) AS users,
    FROM
        `bigquery-public-data.google_analytics_sample.ga_sessions_*`,
        UNNEST(hits) AS hits,
        UNNEST(hits.product) AS product
    WHERE
        _TABLE_SUFFIX BETWEEN '20160801' AND '20170801'
    AND
        (
        hits.eCommerceAction.action_type != '0'
        AND
        hits.eCommerceAction.action_type != '3'
        AND
        hits.eCommerceAction.action_type != '4'
        )
    GROUP BY
        category, action
    ORDER BY
        users DESC
    """
    start = time.time()
    conversion_funnel = client.query(query).result().to_arrow()
    pq.write_table(conversion_funnel, 'conversion_funnel.parquet')
    print(f'conversion_funnel.parquet ready, time taken: {time.time() - start}s')

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
    start = time.time()
    price_sensitive = client.query(query).result().to_arrow()
    pq.write_table(price_sensitive, 'price_sensitive.parquet')
    print(f'price_sensitive.parquet ready, time taken: {time.time() - start}s')

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
    start = time.time()
    by_region = client.query(query).result().to_arrow()
    pq.write_table(by_region, 'by_region.parquet')
    print(f'by_region.parquet ready, time taken: {time.time() - start}')

    query = '''
    SELECT
        product.v2ProductCategory AS product_category,
        SUM(IFNULL(product.productQuantity,0)) AS sales_qty,
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
        sales_qty DESC, revenue DESC;
    '''
    start = time.time()
    by_category = client.query(query).result().to_dataframe()
    by_category = cat_cleaning.clean_categories(by_category, 'product_category')
    by_category.to_parquet('by_category.parquet')
    print(f'by_category.parquet ready, time taken: {time.time() - start}s')

    query = """
    SELECT
        CASE WHEN hits.eCommerceAction.action_type = '1' THEN 'Click through of product lists'
              WHEN hits.eCommerceAction.action_type = '2' THEN 'Product detail views'
              WHEN hits.eCommerceAction.action_type = '5' THEN 'Check out'
              WHEN hits.eCommerceAction.action_type = '6' THEN 'Completed purchase'
        END AS action,
        CASE WHEN product.v2ProductCategory LIKE '%Office%'
          OR product.v2ProductCategory IN ('Notebooks & Journals', 'Writing') THEN 'Office'
        WHEN product.v2ProductCategory LIKE '%Apparel%'
          OR product.v2ProductCategory LIKE '%Wearables%'
          OR product.v2ProductCategory LIKE '%Men%' THEN 'Apparel'
        WHEN product.v2ProductCategory LIKE '%Bags%'
          OR product.v2ProductCategory IN ('Backpacks', 'More Bags') THEN 'Bags'
        WHEN product.v2ProductCategory LIKE '%Brand%'
          OR product.v2ProductCategory IN ('Apple', 'YouTube', 'Waze', 'Google') THEN 'Brands'
        WHEN product.v2ProductCategory LIKE '%Drinkware%'
          OR product.v2ProductCategory LIKE '%Fun%'
          OR product.v2ProductCategory IN ('Mugs', 'Fruit Games',
            'Housewares', 'Headgear', 'Headwear', 'Tumblers') THEN 'Accessories'
        WHEN product.v2ProductCategory LIKE '%Lifestyle%' THEN 'Lifestyle'
        WHEN product.v2ProductCategory LIKE '%Electronics%' THEN 'Electronics'
        WHEN product.v2ProductCategory IN ('Home', '${escCatTitle}', '(not set)',
        '${productitem.product.origCatName}')
           THEN 'Unavailable'
        WHEN product.v2ProductCategory LIKE '%Sale%' THEN 'Sale'
        ELSE 'Other'
        END AS category,
        COUNT(fullVisitorID) AS users,
    FROM
        `bigquery-public-data.google_analytics_sample.ga_sessions_*`,
        UNNEST(hits) AS hits,
        UNNEST(hits.product) AS product
    WHERE
        _TABLE_SUFFIX BETWEEN '20160801' AND '20170801'
    AND
        (
        hits.eCommerceAction.action_type != '0'
        AND
        hits.eCommerceAction.action_type != '3'
        AND
        hits.eCommerceAction.action_type != '4'
        )
    GROUP BY
        category, action
    ORDER BY
        users DESC
    """
    start = time.time()
    conversion_funnel = client.query(query).result().to_arrow()
    pq.write_table(conversion_funnel, 'conversion_funnel.parquet')
    print(f'conversion_funnel.parquet ready, time taken: {time.time() - start}s')

    query = '''
    SELECT
      trafficSource.medium AS channel,
      product.productCategory AS category,
      COUNT(DISTINCT fullVisitorId) AS total_visitors,
      COUNTIF(totals.transactions > 0) AS total_conversions,
      SAFE_DIVIDE(COUNTIF(totals.transactions > 0), COUNT(DISTINCT fullVisitorId)) * 100 AS conversion_rate
    FROM
      `bigquery-public-data.google_analytics_sample.ga_sessions_*`,
      UNNEST(hits) AS hits,
      UNNEST(hits.product) AS product
    WHERE
      _TABLE_SUFFIX BETWEEN '20160801' AND '20170630'  -- Filter for specific date range if needed
    GROUP BY
      channel, category
    ORDER BY
      conversion_rate DESC
    '''
    start = time.time()
    channel_conversion_rate = client.query(query).result().to_dataframe()
    channel_conversion_rate = cat_cleaning.clean_categories(channel_conversion_rate, 'category')
    channel_conversion_rate.to_parquet('channel_conversion_rate.parquet')
    print(f'channel_conversion_rate.parquet ready, time taken: {time.time() - start}s')
