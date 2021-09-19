import zqd
import praw
import sklearn
import subprocess
import requests
import pprint
import uuid
import json


# Part 1: Reddit -> Zed
# Part 2: Zed -> Scikit-learn Model Training
# Part 3: Scikit-learn Model Inference -> Zed
# Part 4: Zed -> Result Querying and Visualization
# This can loop without losing any data since Zed provides persistence.
# Zed's schema flexibility will allow me to develop without stopping to curate schemas.

# auth reddit
with open("./api.txt", "r") as f:
    cl_id = f.readline().strip()
    cl_key = f.readline().strip()

reddit = praw.Reddit(
    client_id=cl_id,
    client_secret=cl_key,
    user_agent="Pulling data into Zed",
)

print(reddit.read_only)

# create pool
pool_name = f"reddit_{str(uuid.uuid4())[:8]}"
proc_result = subprocess.run(f"zed lake create {pool_name}", shell=True, check=True, capture_output=True)
print(proc_result.stdout.decode("utf-8"))

# get pool id
pool_info = proc_result.stdout.decode("utf-8").split(" ")
assert pool_info[2] == pool_name
pool_id = pool_info[3]
pool_url = f"http://127.0.0.1:9867/pool/{pool_id}/branch/main"
print(f"Pool ID: {pool_id}")

# get some submissions
submissions = reddit.subreddit("all").hot(limit=10)

# load them and their comment trees into Zed
for submission in submissions:
    print("====== SUBMISSION =======")
    # print(submission.title)
    # print(pprint.pprint(vars(submission)))
    submission_dict = str(vars(submission))
    json_submission = json.dumps(submission_dict)
    req_out = requests.post(pool_url, json=json_submission)
    print(req_out)

    submission.comments.replace_more(limit=None)
    for comment in submission.comments.list():
        print("====== COMMENTS =======")
        print(comment.body)
        comment_dict = str(vars(comment))
        json_comment = json.dumps(comment_dict)
        req_out = requests.post(pool_url, json=json_comment)
        print(req_out)










# Now that we have some stuff in a zpool, load it and run some models!
# This data will continue to grow as we ingest more reddit posts.
# We can run text models on the data, and persist the data in another zpool.
# Then we can query results from the zpool, and visualize them.

