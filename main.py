from process import Process
from db import DataBase

path = "/Users/rahman/Downloads/products.csv"

p = Process(path)
df = p.run()


# db = DataBase()
# db.create_table("PRODUCTS")
