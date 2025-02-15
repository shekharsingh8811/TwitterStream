from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
import time
from kafka import SimpleProducer, KafkaClient
from datetime import datetime

## Twiter API Access Details ## 
access_token="361068964-kF9lTfEqRy2fnYbumko62hsNHvfAflmrsn2hWYqj"
access_token_secret="RAqEzm7vaWp8xP5AfvaQ7qaA6ZfOoY0W3b6OSBfkV9FIP"
consumer_key="GxPg2owNZRRgDoUxwwECcoJmP"
consumer_secret="CsqWTRqi0PXhZzQuWi3kwz6Bes2e9mEeZudGXOKUXHOaQVEYrV"

## Taking keywords and topic name as input from user ##
keyword_one = input("Please enter a keyword : ")
keyword_two = input("Please enter another keyword : ")
topic_name = input("Please assign a topic name of your choice : ")
print ("Searching for tweets which contain keywords " + keyword_one + " and " + keyword_two + " !" )

StartTime = datetime.now()
print('\nTime is:',StartTime.strftime("%c"))  ## Prints the start time of Kafka Producer ##

class StdOutListener(StreamListener):
    def on_data(self, data):
        if (time.time() - self.start_time) < self.limit:
            producer.send_messages(topic_name, data.encode('utf-8'))
            print (data)
            return True
        else:
            print
            return False
        
    def on_error(self, status):
        print (status)
    
    def __init__(self, time_limit=20):     ## Setting the run time limit in seconds ##
        self.start_time = time.time()
        self.limit = time_limit
        super(StdOutListener, self).__init__()

kafka = KafkaClient("localhost:9092")
producer = SimpleProducer(kafka)  
l = StdOutListener()
auth = OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)
stream = Stream(auth, l)
stream.filter(track=[keyword_one, keyword_two])

EndTime = datetime.now()
print('\nDone processing at:',EndTime.strftime("%c"))  ## Prints the end time of Kafka Producer ##
