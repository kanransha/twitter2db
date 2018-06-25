import os, sys, json
from requests_oauthlib import OAuth1Session
from pymongo import MongoClient

twitter_session = OAuth1Session(
    os.environ['TWITTER_API_KEY'],
    os.environ['TWITTER_API_SECRET'],
    os.environ['TWITTER_ACCESS_TOKEN'],
    os.environ['TWITTER_ACCESS_SECRET']
)

def getTweets(max_id):
    url = 'https://api.twitter.com/1.1/statuses/user_timeline.json'
    params = {'screen_name' : os.environ['TWITTER_TARGET_NAME'], 'count' : '200'}
    if max_id is not None:
        params['max_id'] = max_id 
    response = twitter_session.get(url, params = params)
    if response.status_code != 200:
        sys.stderr.write('Error: Response Status Code = %d\n' % response.status_code)
        exit(1)
    return json.loads(response.text)

mongo_client = MongoClient()
tweet_collection = mongo_client.twitter.tweets
tweet_collection.remove()
tweet_count = 0
next_max_id = None
while True:
    tweets = getTweets(next_max_id)
    if len(tweets) == 0:
        break
    min_id = -1
    for t in tweets:
        tweet_count += 1
        if min_id == -1 or t['id'] < min_id:
            min_id = t['id']
        tweet_collection.insert(t)
        sys.stdout.write('\rSaved %d tweets to database!' % tweet_count)
    next_max_id = min_id - 1

