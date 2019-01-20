import tweepy
import time
from tweepy import Stream
from tweepy import Stream
from tweepy.streaming import StreamListener
import os
import sendgrid
from sendgrid.helpers.mail import *

#twitter settings 
consumer_key = os.environ.get('TWITTER_CONSUMER_KEY')
consumer_secret = os.environ.get('TWITTER_CONSUMER_SECRET')
access_key = os.environ.get('TWITTER_ACCESS_KEY')
access_secret = os.environ.get('TWITTER_ACCESS_SECRET')

#sendgrid settings
sendgrid_key = os.environ.get('SENDGRID_KEY')
to_email= Email(os.environ.get('SENDGRID_RECIPIENT'))

#constants
hyperlink_format = '<a href="{link}">{text}</a>'
emailing_threshold=20

#variables
last_id=1
emails=[]

auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_key, access_secret)
api = tweepy.API(auth)

def send_email(emails):
    sg = sendgrid.SendGridAPIClient(sendgrid_key)
    from_email = Email("twittercollector@noreply")
    subject = emails[0]['text']
    content = Content("text/html", "<br><br>".join(e['user'] + " : "  + hyperlink_format.format(link=e['url'], text=e['text']) for e in emails))
    mail = Mail(from_email, subject, to_email, content)
    response = sg.client.mail.send.post(request_body=mail.get())
    
    if response.status_code != 202:
        return 'An error occurred: {}'.format(response.body), response.status_code

while True:

    print ("Fetching tweets since_id: " + str(last_id))

    tweets = tweepy.Cursor(api.home_timeline, wait_on_rate_limit=True, tweet_mode='extended', since_id=last_id).items(100)
    for index, tweet in enumerate(tweets):    
        
        last_id = int(tweet.id_str) if last_id < int(tweet.id_str) else last_id    
        
        if tweet.entities['urls']:
            emails.append({'user':tweet.user.screen_name , 'text': tweet.full_text, 'url': tweet.entities['urls'][0]['expanded_url']})
            print ("tweet id :" + tweet.id_str + " last_id: " + str(last_id)  + " extracted: " + str(emails[-1]) + " emails in queue: " + str(len(emails)))

        if len(emails)>=emailing_threshold:
            send_email(emails) 
            emails=[]

        time.sleep(1)