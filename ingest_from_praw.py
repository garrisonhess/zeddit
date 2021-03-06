# This script ingests posts from the Public Reddit API Wrapper (PRAW) into a zpool
import zqd
import praw
import subprocess
import requests
import uuid
import json

# auth reddit
with open("./api.txt", "r") as f:
    cl_id = f.readline().strip()
    cl_key = f.readline().strip()
    cl_user = f.readline().strip()
    cl_pw = f.readline().strip()

reddit = praw.Reddit(
    client_id=cl_id,
    client_secret=cl_key,
    user_agent="ingesting data into Zed for (data) science",
)
reddit.read_only = True

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

# get some submissions
submissions = reddit.subreddit("all").hot(limit=100)
total_comments = 0

# load them and their comment trees into Zed
for sub_idx, submission in enumerate(submissions):
    submission_data = json.dumps(vars(submission), default=lambda o: "")
    req_out = requests.post(pool_url, data=submission_data)

    if req_out.status_code != requests.codes.ok:
        print(req_out)
        print(req_out.json())

    submission.comments.replace_more(limit=None)
    for comm_idx, comment in enumerate(submission.comments.list()):
        total_comments += 1
        comment_data = json.dumps(vars(comment), default=lambda o: "")
        req_out = requests.post(pool_url, data=comment_data)
        print(f"submission index: {sub_idx}, comment index: {comm_idx}, total_comments: {total_comments}")

        if req_out.status_code != requests.codes.ok:
            print(req_out)
            print(req_out.json())
