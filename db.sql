CREATE TABLE reports (
     connection_str VARCHAR(255),
     app VARCHAR(255),
     platform VARCHAR(255),
     country VARCHAR(255),
     impressions INT,
     ad_revenue DECIMAL(10, 2),
     date_field DATE );

import argparse
import mysql.connector
import requests
from datetime import datetime
a = datetime.now()
parser = argparse.ArgumentParser()

# input parameters received via arguments
parser.add_argument("--DATE_START", help="Start date for data collection", type=str)
parser.add_argument("--DATE_END", help="End date for data collection", type=str)
parser.add_argument("--DIMENSIONS", help="the dimensions from which data is required", type=str)
parser.add_argument("--TOKEN", help="Authorization token", type=str)
parser.add_argument("--DB_USERNAME", help="username for mysqldb connection", type=str)
parser.add_argument("--DB_PASSWORD", help="password for mysqldb connection", type=str)
parser.add_argument("--DB_NAME", help="database name for mysqldb connection", type=str)
parser.add_argument("--DB_HOSTNAME", help="host for mysqldb connection", type=str, default='')

args = parser.parse_args()

# Reporting API endpoint
api_endpoint = "https://api.libring.com/v2/reporting/get"

token = f"Token {args.TOKEN}"
# header for authorization
headers = {
    "Authorization": token
}

# params for the API request as per document
params = {
    "allow_mock": "true",
    "period": "custom_date",
    "start_date": args.DATE_START,
    "end_date": args.DATE_END,
    "group_by": "connection,app,platform,country",
    "lock_on_collection": "false",
    "dimensions": args.DIMENSIONS
}

response = requests.get(api_endpoint, headers=headers, params=params)
data = response.json()

db = mysql.connector.connect(
    host=args.DB_HOSTNAME,
    user=args.DB_USERNAME,
    password=args.DB_PASSWORD,
    database=args.DB_NAME
)

cursor = db.cursor()

# Insert data into the SQL database
data_to_insert = []
for item in data["connections"]:
    connection = item["connection"]
    app = item["app"]
    platform = item["platform"]
    country = item["country"]
    impressions = item["impressions"]
    ad_revenue = item["ad_revenue"]
    date = item["date"]

    data_to_insert.append((connection, app, platform, country, impressions, ad_revenue, date))

query = "INSERT INTO reports (connection_str, app, platform, country, impressions, ad_revenue, date_field) " \
        "VALUES (%s, %s, %s, %s, %s, %s, %s)"

cursor.executemany(query, data_to_insert)

db.commit()
db.close()
b = datetime.now()

print((b-a).total_seconds())
