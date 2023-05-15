import json
import boto3
import awswrangler as wr
import pandas as pd

GLUE_DATABASE="lab-database"
TARGET_BUCKET="target-wdxleiilinfgsppl"

def parse_event(event):
    key = event['detail']['object']['key']
    bucket = event['detail']['bucket']['name']
    return key, bucket

def read_object(bucket, key):
    s3 = boto3.resource('s3')
    obj = s3.Object(bucket, key)
    return obj.get()['Body'].read().decode('utf-8')

def create_database():
    databases = wr.catalog.databases()
    if GLUE_DATABASE not in databases.values:
        wr.catalog.create_database(GLUE_DATABASE)
        print(wr.catalog.databases())
    else:
        print(f"Database {GLUE_DATABASE} already exists")

def lambda_handler(event, context):
    key, bucket = parse_event(event)
    object_body = read_object(bucket, key)

    create_database()

    order = json.loads(object_body)
    items = order['order'].pop('items', None)
    items = [dict(item, order_id=order['order']['id']) for item in items]

    order_df = pd.json_normalize(order)
    items_df = pd.json_normalize(items)

    desc = "Order table"

    param = {
        "source": "Order Web Service",
        "class": "e-commerce"
    }

    comments = {
        "customer_id": "Unique customer identifier",
        "name": "Customer name",
        "total": "Total value of the order"
    }

    res = wr.s3.to_parquet(
        df=order_df.astype(str),
        path=f"s3://{TARGET_BUCKET}/data/orders/",
        dataset=True,
        database=GLUE_DATABASE,
        table="orders",
        mode="append",
        partition_cols=["customer_type", "order_shipping_address_state"],
        description=desc,
        parameters=param,
        columns_comments=comments
    )

    res = wr.s3.to_parquet(
        df=items_df.astype(str),
        path=f"s3://{TARGET_BUCKET}/data/items",
        dataset=True,
        database=GLUE_DATABASE,
        table="items",
        mode="append",
        partition_cols=["name"]
    )