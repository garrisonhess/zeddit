# This script ingests posts from the Public Reddit API Wrapper (PRAW) into a zpool
import zqd
import praw
import subprocess
import requests
import pprint
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

# load them and their comment trees into Zed
for submission in submissions:
    # unpack submission object into a JSON-serializable dictionary
    submission_dict = {"id": submission.id
                    , "title": submission.title
                    , "score": submission.score
                    , "url": submission.url
                    , "subreddit": submission.subreddit.display_name
                    , "author": submission.author.name
                    , "upvote_ratio": submission.upvote_ratio
                    , "score": submission.score
                    , "ups": submission.ups
                    , "link_flair_css_class": submission.link_flair_css_class
                    , "pwls": submission.pwls
                    , "downs": submission.downs
                    , "quarantine": submission.quarantine
                    , "upvote_ratio": submission.upvote_ratio
                    , "total_awards_received": submission.total_awards_received
                    , "created": submission.created
                    , "score": submission.score
                    , "subreddit_subscribes": submission.subreddit_subscribers
                    , "url": submission.url
                    }

    req_out = requests.post(pool_url, json=submission_dict)
    pprint.pprint(req_out)

    # add in comment ingestion code later... requires schema curation...
    # submission.comments.replace_more(limit=None)
    # for comment in submission.comments.list():
    #     print("====== COMMENTS =======")
    #     print(comment.body)
    #     comment_dict = str(vars(comment))
    #     json_comment = json.dumps(comment_dict)
    #     req_out = requests.post(pool_url, json=json_comment)
    #     print(req_out)





# NOTE: there may be a way around the JSON serialization issues.
# Either it can be handled by Zed, or it can be handled by the user.
# default = lambda o: f"<<non-serializable: {type(o).__qualname__}>>"