import requests

#define my api endpoint
url = "https://dummyjson.com/products"

# send a request to the API
response = requests.get(url)

# convert the request to JSON
data = response.json()

# get products from the json data
#list of dicts
products = data["products"]

