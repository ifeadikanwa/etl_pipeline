import requests
import pandas as pd

#define my api endpoint
url = "https://dummyjson.com/products"

# send a request to the API
response = requests.get(url)

# convert the request to JSON
data = response.json()

# get products from the json data
#list of dicts
products = data["products"]



# convert products list into a dataframe
df = pd.DataFrame(products)

# clean our data by selecting only relevant columns
df = df[["id",
         "title",
         "price",
         "discountPercentage",
         "category",
         "rating",
         "stock",
         ]]

print(df.head())

# transform our data
# add didscounted price column
df["discounted_price"] = df["price"] * (1 - (df["discountPercentage"]/100))

print(df.head())
print(df.info())