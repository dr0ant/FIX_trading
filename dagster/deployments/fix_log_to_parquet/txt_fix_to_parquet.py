import hmac
import hashlib
import requests
import psycopg2
import json
from dagster import op, job, In, Out, get_dagster_logger

# Replace with your actual API key and secret
API_KEY = "vJRo1z7pjFOnS2mH26cv3LMfq0tZgUNzN0uVCuRZ4U2YGXY2Vw2w6gUyZ2qEqFUf"
SECRET_KEY = "LlA2NpnWC0dg96HZfzrIzBR9pcXd18CCrD9rFl6vdTi3yw17Sk468v3oFbCgKDCd"

DB_HOST = '172.24.0.6'
DB_NAME = 'wizecosm_NAS'
DB_USER = 'dr0ant'
DB_PASSWORD = '°889'
DB_PORT = '5432' #inside the docker

BASE_URL = "https://api.binance.com/api/v3/openOrders"

# Set up logging
logger = get_dagster_logger()

@op(out={"open_orders": Out(dagster_type=list)})
def get_binance_open_orders(context):
    logger.info("Fetching Binance open orders...")
    try:
        # Get current server time
        server_time = requests.get("https://api.binance.com/api/v3/time").json()["serverTime"]

        # Create query string
        query_string = f"timestamp={server_time}"

        # Generate signature
        signature = hmac.new(
            SECRET_KEY.encode("utf-8"),
            query_string.encode("utf-8"),
            hashlib.sha256
        ).hexdigest()

        # Construct final URL with query string and signature
        url = f"{BASE_URL}?{query_string}&signature={signature}"

        # Make the GET request to Binance API
        headers = {"X-MBX-APIKEY": API_KEY}
        response = requests.get(url, headers=headers)

        # Get the response data (open orders)
        open_orders = response.json()

        logger.info(f"Fetched {len(open_orders)} open orders.")
        return open_orders

    except Exception as e:
        logger.error(f"Error fetching open orders: {e}")
        raise

@op
def insert_open_orders_into_db(context, open_orders):
    logger.info("Inserting open orders into the database...")
    try:
        # Connect to PostgreSQL database
        conn = psycopg2.connect(
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT
        )
        cursor = conn.cursor()
        logger.info("Connection to Postgres Successfull")
        

        # Drop the table if it exists and create it if not
        cursor.execute("""
            DROP TABLE IF EXISTS wizecrypto.current_open_orders_binance CASCADE;
        """)
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS wizecrypto.current_open_orders_binance (
                order_id BIGINT PRIMARY KEY,
                symbol VARCHAR(20),
                price DECIMAL(18, 8),
                qty DECIMAL(18, 8),
                status VARCHAR(20),
                side VARCHAR(10),
                type VARCHAR(10),
                time BIGINT
            );
        """)
        
        cursor.execute("""
            CREATE VIEW wizecrypto.vw_volume_open_orders_sell AS
            (
                SELECT *
                FROM (
                    SELECT opo.symbol, SUM(opo.price * opo.qty) AS sum_per_symbol
                    FROM wizecrypto.current_open_orders_binance AS opo
                    WHERE opo.side = 'SELL'
                    GROUP BY opo.symbol
                    ORDER BY sum_per_symbol DESC
                ) AS A
                UNION
                SELECT *
                FROM (
                    SELECT '---------TOTAL------', SUM(opo.price * opo.qty) AS sum_per_symbol
                    FROM wizecrypto.current_open_orders_binance AS opo
                    WHERE opo.side = 'SELL'
                ) AS b
                ORDER BY sum_per_symbol DESC
            );
        """)

        # Insert new open orders into the table
        for order in open_orders:
            cursor.execute("""
                INSERT INTO wizecrypto.current_open_orders_binance (order_id, symbol, price, qty, status, side, type, time)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                order.get("orderId"),
                order.get("symbol"),
                order.get("price"),
                order.get("origQty"),
                order.get("status"),
                order.get("side"),
                order.get("type"),
                order.get("time")
            ))

        # Commit the transaction
        conn.commit()
        logger.info("Inserted open orders into database successfully.")

    except Exception as e:
        logger.error(f"Error while connecting to PostgreSQL or inserting data: {e}")
        raise

    finally:
        if conn:
            cursor.close()
            conn.close()

@op
def query_volume_open_orders_sell(context):
    logger.info("Querying volume of open sell orders from the database...")
    try:
        # Connect to PostgreSQL database
        conn = psycopg2.connect(
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT
        )
        cursor = conn.cursor()
        logger.info("Connection to Postgres Successfull")

        # Query the vw_volume_open_orders_sell view
        cursor.execute("""
            SELECT * FROM wizecrypto.vw_volume_open_orders_sell;
        """)

        # Fetch all results
        result = cursor.fetchall()

        logger.info(f"Fetched {len(result)} rows from vw_volume_open_orders_sell.")
        return result

    except Exception as e:
        logger.error(f"Error while querying the vw_volume_open_orders_sell view: {e}")
        raise

    finally:
        if conn:
            cursor.close()
            conn.close()

@op
def display_result(context, result):
    logger.info("Displaying the result...")
    if result:
        for row in result:
            logger.info(row)
    else:
        logger.info("No results to display.")

@job
def binance_data_pipeline():
    open_orders = get_binance_open_orders()
    insert_open_orders_into_db(open_orders)
    volume_data = query_volume_open_orders_sell()
    display_result(volume_data)
