import requests
import pandas as pd
import sqlite3
from datetime import datetime

# extract data
def extract():
    # define my api endpoint
    base_url = "https://dummyjson.com/products"
    limit = 30
    skip = 0
    all_products = []
    
    while True:
        url = f"{base_url}?limit={limit}&skip={skip}"

        # send a request to the API
        response = requests.get(url)

        # convert the request to JSON
        data = response.json()

        # get products from the json data
        # list of dicts
        products = data["products"]
        total = data["total"]
        
        # if there aare no products, exit loop
        if not products:
            break
        
        # add products we just fetched to the cumulative list
        all_products.extend(products)
        print(f"Fetched {len(all_products)} of {total} products")
        
        skip += limit
        
        # if we fetched the total products already, exit loop
        if(len(all_products) >= total):
            break
        
    
    return all_products


# transform data
def transform(products):
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
    
    # ensure that price is numeric
    df["price"] = pd.to_numeric(df["price"], errors="coerce")

    print(df.head())

    # transform our data
    # add didscounted price column
    df["discounted_price"] = df["price"] * (1 - (df["discountPercentage"] / 100))

    return df

# load our data into the database
def load(df):
    # connect to products database (create new or open existing db)
    connection = sqlite3.connect("products.db")

    # load our dataframe into products table
    df.to_sql("products", connection, if_exists="replace", index=False)

    print("data loaded into database")

    # close connection
    connection.close()


#pipeline runner
def run_pipeline():
    print(f"Pipeline started at {datetime.now()}")

    extracted_products = extract()
    dataframe = transform(products=extracted_products)
    load(df=dataframe)

    print(f"Pipeline finished at {datetime.now()}")


if __name__ == "__main__":
    run_pipeline()
