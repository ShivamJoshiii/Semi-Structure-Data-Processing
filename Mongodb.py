import tweepy
import json
from tweepy import Stream
from tweepy import OAuthHandler
from tweepy.streaming import StreamListener
import pymongo
from pymongo import MongoClient
 
access_token = 'your token here'
access_token_secret = 'your token here'
consumer_token = 'your token here'
consumer_token_secret = 'your token here'
 
auth = tweepy.OAuthHandler(consumer_token, consumer_token_secret)  
auth.set_access_token(access_token, access_token_secret)  
api = tweepy.API(auth)

conn = MongoClient('localhost', 27017)
db = conn.twitter
db.tweets.create_index([("id", pymongo.ASCENDING)],unique = True,)
collection = db.twitter

try:  
    for tweet in tweepy.Cursor(api.search, q='Presidential Election',lang="en").items(50000):
#                    tweet_text = tweet.text  
                    time = tweet.created_at  
#                    tweeter = tweet.user.screen_name  
#                    tweet_dict = {"tweet_text" : tweet_text.strip(), "timestamp" : str(time), "user" :tweeter,
#                                 "location" : tweet.user.location,"entities" : tweet.user.entities}  
                    tweet_json = json.dumps(tweet._json)  
                    print(tweet_json)
                    try:
                        with open("Presidential Election.json","a") as file: #appending the JSON data to the file
                            file.write(tweet_json+",\n")
                    except:
                        pass
except tweepy.TweepError:  
    time.sleep(60)

class streamdata(StreamListener):
    
    def on_data(self, data):
        # Load the Tweet into the variable "tweet"
        try:
            
            tweet = json.loads(data)
            # Pull important data from the tweet to store in the database.
            tweet_id = tweet['id_str']  
            tweet_time = tweet['retweeted_status']['created_at']
            username = tweet['user']['screen_name']
            person_name = tweet['user']['name']
            followers_count = tweet['user']['followers_count']  
            friends_count = tweet['user']['friends_count']
            user_location = tweet['user']['location']
            text = tweet['text']  
            hashtags = tweet['entities']['hashtags']
                
            # Load all of the extracted Tweet data into the variable "tweet" that will be stored into the database
            tweet_obj = {
                    'id':tweet_id,  
                    'created':tweet_time,
                    'username':username, 
                    'name':person_name,
                    'followers':followers_count,
                    'friends': friends_count,
                    'tweet_text':text,
                    'location':user_location,
                    'hashtags':hashtags
                    }
                
            # Save the refined Tweet data to MongoDB
            collection.insert_one(tweet_obj)      
            return True
        except:
            pass
if __name__ == "_main_":
    authentication = OAuthHandler(consumer_token, consumer_token_secret)
    authentication.set_access_token(access_token, access_token_secret)
    tweeStream = Stream(authentication, streamdata(num_tweets=7500))
    tweeStream.filter(track=["Presidential Election",],languages=['en'])
