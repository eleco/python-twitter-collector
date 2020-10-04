import time
import json
import os
import tweepy
from tweepy import Stream
from tweepy.streaming import StreamListener
import redis
import requests
from lxml.html import fromstring
import pymongo
from pymongo import MongoClient

#twitter settings 
consumer_key = os.environ.get('TWITTER_CONSUMER_KEY')
consumer_secret = os.environ.get('TWITTER_CONSUMER_SECRET')
access_key = os.environ.get('TWITTER_ACCESS_KEY')
access_secret = os.environ.get('TWITTER_ACCESS_SECRET')

#mailgun settings
mailgun_key = os.environ.get('MAILGUN_KEY')
mailgun_sandbox= os.environ.get('MAILGUN_SANDBOX')
to_email= os.environ.get('MAILGUN_RECIPIENT')


#heroku specific
ON_HEROKU = 'ON_HEROKU' in os.environ

#constants
hyperlink_format = '<a href="{link}">{text}</a>'
emailing_threshold=10

#variables
emails=[]
last_tweet_id=1

#mongo
mongodb_url = os.environ.get('MONGO_URL')
mongo_db = os.environ.get('MONGO_DB')
mongoclient = MongoClient(mongodb_url)
mongodb = mongoclient[mongo_db]

auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_key, access_secret)
api = tweepy.API(auth)


def format_email(emails):
    html="<html><body>"
    for e in emails:
        text = e['text'].strip().replace('\n\n', '\n')
        links =''.join(( hyperlink_format.format(link=l['url'], text=l['title']) for l in e['links']))
        html +=  "<p>" + e['user'] + "<br>" + text + "<br>" + links + "</p>"
    html+="</body></html>"
    return html


def send_email(emails):
    try:
        request_url = 'https://api.mailgun.net/v2/{0}/messages'.format(mailgun_sandbox)
        request = requests.post(request_url, auth=('api', mailgun_key), data={
            'from': 'me@twittercollector.xyz',
            'to': to_email,
            'subject': emails[0]['text'],
            'html': format_email(emails)
        })

        print ('Status: {0}'.format(request.status_code))
        print ('Body:   {0}'.format(request.text))
    except Exception as e:
       print('An error occurred: ',e)



def fetch_title(url):
    try:
        r = requests.get(url)
        tree = fromstring(r.content)
        title = tree.findtext('.//title')
        return title.strip().replace('\n\n', '\n') if title is not None else 'no title'
    except:
        print('unable to fetch title from url:', url)
        return "no title"
       

if ON_HEROKU:
    db=redis.from_url(os.environ['REDIS_URL'])    
    last_tweet_id = db.get('last_tweet_id') or '1'

print("*** starting up ***")

while True:

    print ("Fetching tweets since_id: " + str(last_tweet_id))

    tweets = tweepy.Cursor(api.home_timeline, wait_on_rate_limit=True, tweet_mode='extended', since_id=last_tweet_id).items(100)
    for index, tweet in enumerate(tweets):    
        
        last_tweet_id = int(tweet.id_str) if int(last_tweet_id) < int(tweet.id_str) else last_tweet_id    
        
        if tweet.entities['urls']:
            links=[]
            full_text = tweet.full_text
           
            for tweet_url in tweet.entities['urls']:
                title = fetch_title(tweet_url['expanded_url'])
                links.append({"title":title, "url": tweet_url['expanded_url']})
                full_text = full_text.replace(tweet_url['url'],'')


            dict ={'tweetid': tweet.id_str, 'user':tweet.user.screen_name , 'text': full_text,  "links":links}
            
            #store in mongo
            mongo_tweets = mongodb.tweets
            mongo_tweets.insert_one(dict)               
            
            #send email
            emails.append(dict)
            if len(emails)>=emailing_threshold:
                send_email(emails) 
                emails=[]

            print ("tweet id :" + tweet.id_str + " last_id: " + str(last_tweet_id)  + " emails in queue: " + str(len(emails)))



    if ON_HEROKU:
        db.set('last_tweet_id',last_tweet_id)

    time.sleep(60*10)