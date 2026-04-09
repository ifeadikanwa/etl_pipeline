import requests
import pandas as pd
import sqlite3
from datetime import datetime

# extract data
def extract():
    # define my api endpoint
    url = "https://dummyjson.com/products"

    # send a request to the API
    response = requests.get(url)

    # convert the request to JSON
    data = response.json()

    # get products from the json data
    # list of dicts
    products = data["products"]

    return products


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
