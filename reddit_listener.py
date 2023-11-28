import json
import socket
import praw
import pandas as pd

with open("config.json", "r") as jsonfile:
    data = json.load(jsonfile) # Reading the config file
    print("Config data read successful",data)

reddit = praw.Reddit(
        client_id = data["client_id"],
        client_secret = data["client_secret"],
        user_agent="COM3021 Reddit Listener")

stream = reddit.subreddit("AskUK+AskAnAmerican").stream

cols = ["id","author", "submission", "body","subreddit","created_utc","collected_utc"]
new_df = pd.DataFrame(columns=cols)

count = 0
for comments in stream.comments(skip_existing=True):
    count += 1
    author = comments.author
    comment_id = comments.link_id
    submission = comments.id
    body = comments.body
    subreddit = comments.subreddit
    created = comments.created_utc
    collected = comments.created

    new_post = [comment_id,
                author,
                submission,
                body,
                subreddit,
                created,
                collected]

    new_df.loc[count] = new_post
    new_df.to_csv("UKandUS.csv")

