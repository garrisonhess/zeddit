# This script ingests posts from the Pushshift.io (a reddit archive) into a new zpool
import zqd
import subprocess
import requests
import uuid

# Endpoints
# https://api.pushshift.io/reddit/submission/search/
# https://api.pushshift.io/reddit/comment/search/

# Examples, news subreddit in July 2018
# https://api.pushshift.io/reddit/submission/search/?subreddit=news&after=2018-07-01&before=2018-08-01
# https://api.pushshift.io/reddit/comment/search/?subreddit=news&after=2018-07-01&before=2018-08-01


# create pool
pool_name = f"reddit_{str(uuid.uuid4())[:8]}"
proc_result = subprocess.run(f"zed lake create {pool_name}", shell=True, check=True, capture_output=True)
print(proc_result.stdout.decode("utf-8"))

# get pool id
pool_info = proc_result.stdout.decode("utf-8").split(" ")
assert pool_info[2] == pool_name
pool_id = pool_info[3].strip()
pool_url = f"http://127.0.0.1:9867/pool/{pool_id}/branch/main"
print(f"Pool ID: {pool_id}")
print(f"Pool URL: {pool_url}")

# get data from pushshift
url1 = "https://api.pushshift.io/reddit/submission/search/?subreddit=news&after=2018-07-01&before=2018-08-01"
url2 = "https://api.pushshift.io/reddit/comment/search/?subreddit=news&after=2018-07-01&before=2018-08-01"

submission_result = requests.get(url1)
comment_result = requests.get(url2)

req_out = requests.post(pool_url, json=submission_result.json())
req_out = requests.post(pool_url, json=comment_result.json())



