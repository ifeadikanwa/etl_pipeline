import requests
import pandas as pd
import sqlite3
import logging
from datetime import datetime

#logging
logging.basicConfig(
    filename="pipeline.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

# extract data
def extract():
    # define my api endpoint
    base_url = "https://dummyjson.com/products"
    limit = 30
    skip = 0
    all_products = []
    
    try:
        
        while True:
            url = f"{base_url}?limit={limit}&skip={skip}"
            
            # send a request to the API
            response = requests.get(url, timeout=10)
            response.raise_for_status()

            # convert the request to JSON
            data = response.json()

            # get products from the json data
            # list of dicts
            products = data["products"]
            total = data["total"]
            
            # if there aare no products, exit loop
            if not products:
                logging.warning("No products returned from API")
                break
            
            # add products we just fetched to the cumulative list
            all_products.extend(products)
            logging.info(f"Fetched {len(all_products)} of {total} products")
            print(f"Fetched {len(all_products)} of {total} products")
            
            skip += limit
            
            # if we fetched the total products already, exit loop
            if(len(all_products) >= total):
                break 
        
        return all_products
 
    except requests.exceptions.RequestException as error:
            logging.error(f"API request failed: {error}")
            print(f"API request failed: {error}")
            return []


# transform data
def transform(products):
    try:
        if not products:
            logging.warning("No products available for transformation")
            return pd.DataFrame()
        
        # convert products list into a dataframe
        df = pd.DataFrame(products)

        # clean our data by selecting only relevant columns
        df = df[
            [
                "id",
                "title",
                "price",
                "discountPercentage",
                "category",
                "rating",
                "stock",
            ]
        ]
        
        #remove missing values
        df = df.dropna()
        
        # ensure that numeric columns are numeric
        df["price"] = pd.to_numeric(df["price"], errors="coerce")
        df["discountPercentage"] = pd.to_numeric(df["discountPercentage"], errors="coerce")

        # remove rows where numeric conversion failed
        df = df.dropna(subset=["price", "discountPercentage"])

        # transform our data
        # add didscounted price column
        df["discounted_price"] = df["price"] * (1 - (df["discountPercentage"] / 100))

        logging.info(f"Transformed {len(df)} records successfully")
        print(df.head())
        
        return df
    
    except Exception as error:
        logging.error(f"Transformation failed: {error}")
        print(f"Transformation failed: {error}")
        return pd.DataFrame()

# load our data into the database
def load(df):
    if df.empty:
        logging.warning("Load skipped because dataframe is empty")
        print("Load skipped because dataframe is empty")
        return
    
    connection = None
     
    try:
        
        # connect to products database (create new or open existing db)
        connection = sqlite3.connect("products.db")

        # load our dataframe into products table
        df.to_sql("products", connection, if_exists="replace", index=False)

        logging.info(f"Loaded {len(df)} records into products table")
        print("data loaded into database")

    except sqlite3.Error as error:
        logging.error(f"Database load failed: {error}")
        print(f"Database load failed: {error}")

    finally:
        if connection:
            connection.close()
            logging.info("Database connection closed")

    # close connection
    connection.close()


#pipeline runner
def run_pipeline():
    logging.info("Pipeline started")
    print(f"Pipeline started at {datetime.now()}")

    try:
        extracted_products = extract()
        dataframe = transform(extracted_products)
        load(dataframe)

        logging.info("Pipeline finished successfully")
        print(f"Pipeline finished at {datetime.now()}")

    except Exception as error:
        logging.error(f"Pipeline failed: {error}")
        print(f"Pipeline failed: {error}")



if __name__ == "__main__":
    run_pipeline()
