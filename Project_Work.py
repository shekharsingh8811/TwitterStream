########################################## Trending Topics #########################################

import tweepy
from tweepy import OAuthHandler
import pandas as pd
from pymongo import MongoClient 

## Fetching details for city and its woeid into memory ##
client = MongoClient('localhost:27017')
datab = client.DataEngineering
collection = client.DataEngineering.CityNames

city_details = collection.find({}, {"_id" :0, "city" :1, "woeid" :1})
formated_city_details = pd.DataFrame.from_records(city_details)

## Twiter API Access Details ## 
access_token="361068964-kF9lTfEqRy2fnYbumko62hsNHvfAflmrsn2hWYqj"
access_token_secret="RAqEzm7vaWp8xP5AfvaQ7qaA6ZfOoY0W3b6OSBfkV9FIP"
consumer_key="GxPg2owNZRRgDoUxwwECcoJmP"
consumer_secret="CsqWTRqi0PXhZzQuWi3kwz6Bes2e9mEeZudGXOKUXHOaQVEYrV"

auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)
api = tweepy.API(auth)

## Taking city name as an input from user ##
city_name = input("Please enter the city name of your choice: ")
print ("\nBelow is the trending topics for city " + city_name + " !" )
for index in range(0,467):
    if city_name == formated_city_details['city'][index]:
        woe_id = formated_city_details['woeid'][index] ## Taking woeid of the city name provided by user ##

print("\n")
trending_topics = api.trends_place(woe_id) 
data = trending_topics[0] 
trends = data['trends'] ## fething the trends ##
names = [trend['name'] for trend in trends] ## fetching the name from each trend ##
trendNames = ' '.join(names) ## joining all the trend names together with a ' ' separating them ##
print(trendNames)

######################################## Kafka Producer #########################################

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
keyword_one = input("Please enter a keyword: ")
keyword_two = input("\nPlease enter another keyword: ")
topic_name = input("\nPlease assign a topic name of your choice: ")
print ("\nSearching for tweets which contain keywords " + keyword_one + " and " + keyword_two + " !" )

StartTime = datetime.now()
print("\nKafka Producer has been started at: ",StartTime.strftime("%c"))  ## Prints the start time of Kafka Producer ##

class StdOutListener(StreamListener):
    def on_data(self, data):
        if (time.time() - self.start_time) < self.limit:
            producer.send_messages(topic_name, data.encode('utf-8'))
            #print (data)
            return True
        else:
            print
            return False
        
    def on_error(self, status):
        print (status)
    
    def __init__(self, time_limit=60):     ## Setting the run time limit in seconds ##
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
print("\nKafka Producer has been stopped at: ",EndTime.strftime("%c"))  ## Prints the end time of Kafka Producer ##

######################################## Kafka Consumer #########################################

from kafka import KafkaConsumer
from pymongo import MongoClient
from json import loads
from datetime import datetime

## Taking topic name to be consumed as input from user ##
topic_name = input("Please enter topic name which you want Kafka to consume: ")
print ("\nConsuming data for topic " + topic_name + " !" )

TweetsWritten = 0
StartTime = datetime.now()
print("\nKafka Consumer has been started at: ",StartTime.strftime("%c"))  ## Prints the start time of Kafka Consumer ##

consumer = KafkaConsumer(
    topic_name,        ## topic name in Kafka ##
     bootstrap_servers=['localhost:9092'],
     auto_offset_reset='earliest',
     enable_auto_commit=True,
     consumer_timeout_ms= 30 * 1000,
     #group_id='my-group',
     value_deserializer=lambda x: loads(x.decode('utf-8')))

## Setting up connection with MongoDB ##
client = MongoClient('localhost:27017')
db = client.DataEngineering
collection = client.DataEngineering.topic_name
db.topic_name.drop()

for tweet in consumer:
    tweet = tweet.value
    collection.insert_one(tweet)
    TweetsWritten = TweetsWritten + 1
    print("\nTweet number %d written" %(TweetsWritten))

print("\nWritten %d tweets into MongoDB" %(TweetsWritten)) ## Prints the number of records written into MongoDB ##

EndTime = datetime.now()
print("\nKafka Consumer has been stopped at: ",EndTime.strftime("%c"))  ## Prints the end time of Kafka Consumer ##

######################################## Analysis #########################################

import pandas as pd
import matplotlib.pyplot as plt
from pymongo import MongoClient 

client = MongoClient('localhost:27017')
db = client.DataEngineering
collection = client.DataEngineering.topic_name

print("List of top 10 hashtags used in these tweets:\n")
hashtag_count = collection.aggregate([{'$unwind' : '$entities.hashtags'}, 
                                      {'$group' : {'_id' : '$entities.hashtags.text', 'TagCount' : {'$sum' : 1}}}, 
                                      {'$sort' : {'TagCount' : -1}}, 
                                      {'$limit' : 10}, 
                                      {'$project' : {'hashtags' : '$_id', 'TagCount' : 1, '_id' : 0}}])
hashtag = pd.DataFrame.from_records(hashtag_count)
print(hashtag)

#Plotting the graph
hashtag_plot=hashtag.plot.bar(x='hashtags', y='TagCount', title="Top 10 Hashtags", width=0.75, figsize=(15, 10), 
                              rot=25, color='turquoise')
hashtag_plot.set_xlabel("Hashtags", fontsize=20)
hashtag_plot.set_ylabel("TagCount", fontsize=20)

for data in hashtag_plot.patches:
    hashtag_plot.annotate(int(data.get_height()), (data.get_x() * 1.0, data.get_height() * 1.01))
plt.show()
    
######################################## Analysis #########################################

import pandas as pd
import matplotlib.pyplot as plt
from pymongo import MongoClient 

client = MongoClient('localhost:27017')
db = client.DataEngineering
collection = client.DataEngineering.topic_name

print("List of top 10 languages used in these tweets apart from English and Undefined:\n")
language_count = collection.aggregate([{'$group' : {'_id' : '$lang', 'count' : {'$sum' : 1}}}, 
                                       {'$match' : {'count' : {'$gt' : 1}}}, 
                                       {'$sort' : {'count' : -1}}, 
                                       {'$limit' : 12}, 
                                       {'$project' : {'language' : '$_id', 'count' : 1, '_id' : 0}}])
language = pd.DataFrame.from_records(language_count)
language = language.iloc[2:]
print(language)

#Plotting the graph
language_plot=language.plot.bar(x='language', y='count', title="Top 10 Languages", width=0.75, figsize=(15, 10), 
                                rot=25, color='teal')
language_plot.set_xlabel("Language", fontsize=20)
language_plot.set_ylabel("Count", fontsize=20)

for data in language_plot.patches:
    language_plot.annotate(int(data.get_height()), (data.get_x() * 1.0, data.get_height() * 1.01))
plt.show()

######################################## Analysis #########################################

import pandas as pd
import matplotlib.pyplot as plt
from pymongo import MongoClient 

client = MongoClient('localhost:27017')
db = client.DataEngineering
collection = client.DataEngineering.topic_name

print("List of top 10 locations that these tweets have been tweeted from:\n")
location_count = collection.aggregate([{'$group' : {'_id' : '$user.location', 'count' : {'$sum' : 1}}}, 
                                       {'$match' : {'count' : {'$gt' : 1}}}, 
                                       {'$sort' : {'count' : -1}}, 
                                       {'$limit' : 10}, 
                                       {'$project' : {'location' : '$_id', 'count' : 1, '_id' : 0}}])
location = pd.DataFrame.from_records(location_count)
location = location.iloc[1:]
print(location)

#Plotting the graph
location_plot=location.plot.bar(x='location', y='count', title ="Top 10 Locations", width=0.75, figsize=(15, 10), 
                                rot=25, color='violet')
location_plot.set_xlabel("Location", fontsize=20)
location_plot.set_ylabel("Count", fontsize=20)

for data in location_plot.patches:
    location_plot.annotate(int(data.get_height()), (data.get_x() * 1.0, data.get_height() * 1.01))
plt.show()
    
######################################## Analysis #########################################

import pandas as pd
import matplotlib.pyplot as plt
from pymongo import MongoClient 

client = MongoClient('localhost:27017')
db = client.DataEngineering
collection = client.DataEngineering.topic_name

print("List of top 10 sources that these tweets have been tweeted from:\n")
from bs4 import BeautifulSoup
source_count = collection.aggregate([{'$group' : {'_id' : '$source', 'count' : {'$sum' : 1}}}, 
                                     {'$sort' : {'count' : -1}}, 
                                     {'$limit' : 10}, 
                                     {'$project' : {'source' : '$_id', 'count' : 1, '_id' : 0}}])
source_detail = pd.DataFrame.from_records(source_count)
source_name = source_detail.source
source_detail['source'] = [BeautifulSoup(source_name).getText() for source_name in source_detail['source']]
print(source_detail)

#Plotting the graph
source_plot=source_detail.plot.bar(x='source', y='count', title ="Top 10 Sources", width=0.75, figsize=(15, 10), 
                                rot=25, color='purple')
source_plot.set_xlabel("Source", fontsize=20)
source_plot.set_ylabel("Count", fontsize=20)

for data in source_plot.patches:
    source_plot.annotate(int(data.get_height()), (data.get_x() * 1.0, data.get_height() * 1.01))
plt.show()

######################################## Analysis #########################################

import pandas as pd
import matplotlib.pyplot as plt
from pymongo import MongoClient 

client = MongoClient('localhost:27017')
db = client.DataEngineering
collection = client.DataEngineering.topic_name

pd.set_option('max_colwidth', 800)
resulta = collection.aggregate( 
            [
                {"$group": { "_id": { "User Name": "$user.name"}, 
                            "Tweets":{"$last" : "$user.statuses_count"}, 
                            "Followers":{"$last" : "$user.followers_count"}, 
                            "Following":{"$last" : "$user.friends_count"}  }}                               
            ]
        )

resultb = pd.DataFrame.from_records(resulta)
resultb['_id'] = resultb['_id'].astype(str)
#splitting _id to A and B
resultb['A'], resultb['B'] = resultb['_id'].str.split(':', 1).str
#splitting B to C and D
resultb['C'], resultb['D'] = resultb['B'].str.split('}', 1).str
#renaming C to User Name
resultb = resultb.rename(columns = {'C' : 'User Name'})
#rearranging columns
final = resultb[["User Name", "Tweets","Followers", "Following"]]

final2 = final[["User Name", "Tweets","Followers", "Following"]]
#changing var type
final2['Tweets'] = final2['Tweets'].astype(int)
final2['Followers'] = final2['Followers'].astype(int)
final2['Following'] = final2['Following'].astype(int)


#sorting users in descending order by number of tweets
final2 = final2.sort_values('Following', ascending = [0])
print(final2.head(10))

#printing the top 10 users by No. of tweets
Top10Users = final2.head(10)

#Plotting the graph
ax=Top10Users.plot.bar(x='User Name', y=['Followers', 'Following'], title ="Top 10 Users", width = 0.75, 
                       figsize=(22, 10), rot=25)
ax.set_xlabel("User Name", fontsize=20)
ax.set_ylabel("Numbers", fontsize=20)

for p in ax.patches:
    ax.annotate(int(p.get_height()), (p.get_x() * 1.0, p.get_height() * 1.01))
plt.show()

##########################################################################################
