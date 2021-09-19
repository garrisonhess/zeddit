# This script ingests posts from the official Reddit API into a new zpool
import requests
import json

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



# auth reddit
with open("./api.txt", "r") as f:
    cl_id = f.readline().strip()
    cl_key = f.readline().strip()
    cl_user = f.readline().strip()
    cl_pw = f.readline().strip()


auth = requests.auth.HTTPBasicAuth(cl_id, cl_key)
data = {'grant_type': 'password',
        'username': cl_user,
        'password': cl_pw}
headers = {'User-Agent': 'MyBot/0.0.1'}
res = requests.post('https://www.reddit.com/api/v1/access_token',
                    auth=auth, data=data, headers=headers)
TOKEN = res.json()['access_token']

# while the token is valid (~2 hours) we just add headers=headers to our requests
headers = {**headers, **{'Authorization': f"bearer {TOKEN}"}}
requests.get('https://oauth.reddit.com/api/v1/me', headers=headers)



# get some posts
res = requests.get("https://oauth.reddit.com/r/python/hot", headers=headers)


# insert into zed lake
req_out = requests.post(pool_url, json=res.json())