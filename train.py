import zqd
import praw
import sklearn
import subprocess





# Connect to the REST API at the default base URL (http://127.0.0.1:9867).
# To use a different base URL, supply it as an argument.
client = zqd.Client()

# Begin executing a Zed query for all records in the pool named
# "your_pool".  This returns an iterator, not a container.
# records = client.search('from your_pool'):


print(client)

# # Stream records from the server.
# for record in records:
#     print(record)
