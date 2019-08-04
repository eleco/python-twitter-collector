import tweepy
import time
from tweepy import Stream
from tweepy import Stream
from tweepy.streaming import StreamListener
import os
import sendgrid
from sendgrid.helpers.mail import *
import redis
import requests
from lxml.html import fromstring


#twitter settings 
consumer_key = os.environ.get('TWITTER_CONSUMER_KEY')
consumer_secret = os.environ.get('TWITTER_CONSUMER_SECRET')
access_key = os.environ.get('TWITTER_ACCESS_KEY')
access_secret = os.environ.get('TWITTER_ACCESS_SECRET')

#sendgrid settings
sendgrid_key = os.environ.get('SENDGRID_KEY')
to_email= Email(os.environ.get('SENDGRID_RECIPIENT'))

#heroku specific
ON_HEROKU = 'ON_HEROKU' in os.environ

#constants
hyperlink_format = '<a href="{link}">{text}</a>'
emailing_threshold=20

#variables
emails=[]
last_tweet_id=1

auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_key, access_secret)
api = tweepy.API(auth)


def format_email(emails):
    html=""
    for e in emails:
        subject = e['text'].strip().replace('\n\n', '\n')
        links =''.join(( hyperlink_format.format(link=l['url'], text=l['title'].strip().replace('\n\n', '\n')) for l in e['links']))
        html +=  "<p>" + e['user'] + "<br>" + subject + "<br>" + links + "</p>"


def send_email(emails):
    sg = sendgrid.SendGridAPIClient(sendgrid_key)
    from_email = Email("twittercollector@noreply")
    content = Content("text/html", format(emails))
    mail = Mail(from_email, subject, to_email, content)
    response = sg.client.mail.send.post(request_body=mail.get())
    
    if response.status_code != 202:
        return 'An error occurred: {}'.format(response.body), response.status_code


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
                r = requests.get(tweet_url['expanded_url'])
                tree = fromstring(r.content)
                title = tree.findtext('.//title').strip()
                links.append({"title":title, "url": tweet_url['expanded_url']})
                full_text = full_text.replace(tweet_url['url'],'')
                
            emails.append({'user':tweet.user.screen_name , 'text': full_text,  "links":links})
            print ("tweet id :" + tweet.id_str + " last_id: " + str(last_tweet_id)  + " extracted: " + str(emails[-1]) + " emails in queue: " + str(len(emails)))

            if len(emails)>=emailing_threshold:
                send_email(emails) 
                emails=[]

    if ON_HEROKU:
        db.set('last_tweet_id',last_tweet_id)

    time.sleep(60*10)