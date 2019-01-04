from kafka import KafkaConsumer
from pymongo import MongoClient
from json import loads
from datetime import datetime

## Taking topic name to be consumed as input from user ##
topic_name = input("Please enter topic name which you want Kafka to consume :")
print ("Consuming data for topic " + topic_name + " !" )

countDocsWritten = 0
StartTime = datetime.now()
print('\nTime is:',StartTime.strftime("%c"))  ## Prints the start time of Kafka Consumer ##

consumer = KafkaConsumer(
    topic_name,        ## topic name in Kafka ##
     bootstrap_servers=['localhost:9092'],
     auto_offset_reset='earliest',
     enable_auto_commit=True,
     consumer_timeout_ms= 10 * 1000,
     #group_id='my-group',
     value_deserializer=lambda x: loads(x.decode('utf-8')))

## Setting up connection with MongoDB ##
client = MongoClient('localhost:27017')
db = client.DataEngineering
collection = client.DataEngineering.topic_name
db.topic_name.drop()

for message in consumer:
    message = message.value
    collection.insert_one(message)
    countDocsWritten = countDocsWritten + 1
    print('\nWritten %d th record' %(countDocsWritten))

print('\nWritten %d documents to MongoDb' %(countDocsWritten)) ## Prints the number of records written into MongoDB ##
EndTime = datetime.now()
print('\nDone processing at:',EndTime.strftime("%c"))  ## Prints the end time of Kafka Consumer ##
