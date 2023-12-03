import json
import socket
import praw
from praw.models.reddit.subreddit import SubredditStream
from datetime import datetime



class RedditProducer(SubredditStream):
    def __init__(self, subreddit, socket):
        super().__init__(subreddit)
        self.socket = socket
        
    def run(self):
        for comment in self.comments(skip_existing=False):
            ###### your logic goes here #######

            bigtime = str(datetime.utcfromtimestamp(comment.created_utc)).split()
            date = bigtime[0]
            time = bigtime[1]

            data = {
                "author":comment.author.name,
                "body":comment.body,
                "subreddit":comment.subreddit.display_name,
                "created_utc":comment.created_utc,
                "date":date,
                "time":time
            }

            print(data['created_utc'])

            self.socket.send((repr(data) +'\n').encode('utf-8'))




if __name__ == '__main__':

    with open("config.json", "r") as jsonfile:
        data = json.load(jsonfile)  # Reading the config file
        print("Config data read successful", data)

    reddit = praw.Reddit(
            client_id=data["client_id"],
            client_secret=data["client_secret"],
            user_agent="COM3021 Reddit Producer"
    )

    host = '0.0.0.0'
    port = 5590
    address = (host, port)

    #Initializing the socket

    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.bind(address)
    server_socket.listen(5)

    print("Listening for client...")

    conn, address = server_socket.accept()

    print("Connected to Client at " + str(address))



    subreddits = reddit.subreddit("AskUK+AskAnAmerican")
    stream = RedditProducer(subreddits, conn)
    stream.run()

    
