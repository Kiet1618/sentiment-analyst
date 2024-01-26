from confluent_kafka import Consumer, KafkaError
import json
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from pymongo import MongoClient
from pyspark.ml import PipelineModel
from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType, StringType
from pyspark.sql.functions import udf
from pyspark import SparkConf

# Kafka consumer configuration
hadoop_bin_path = r'C:\HADOOP\bin'
consumer_config = {
    'bootstrap.servers': 'localhost:9092',  # Change if Kafka broker is running on a different host
    'group.id': 'youtube_sentiment_group',
    'auto.offset.reset': 'earliest'
}

# MongoDB connection
mongo_client = MongoClient('mongodb+srv://bigdatapyspark:bigdatapyspark@bigdata.d648sgf.mongodb.net/')
db = mongo_client['youtube_sentiment']
combined_collection = db['combined_results']


# Spark session creation
spark = SparkSession.builder.appName("YoutubeSentimentAnalysis") \
    .getOrCreate()

# Create Kafka consumer
consumer = Consumer(consumer_config)

# Subscribe to the Kafka topic
topic = 'youtube_comments'
consumer.subscribe([topic])

# Sentiment analyzer
analyzer = SentimentIntensityAnalyzer()

# Continuously poll for new messages
while True:
    msg = consumer.poll(1.0)  # Adjust the timeout as needed

    if msg is None:
        continue
    if msg.error():
        if msg.error().code() == KafkaError._PARTITION_EOF:
            continue
        else:
            print(msg.error())
            break

    # Process the received message
    try:
        youtube_comment = json.loads(msg.value())

        # Check if 'text' key is present in youtube_comment
        if 'text' not in youtube_comment:
            continue

        # Perform VADER sentiment analysis on youtube_comment['text']
        text = youtube_comment['text']
        sentiment_score = analyzer.polarity_scores(text)

        # Determine sentiment category based on compound score
        if sentiment_score['compound'] >= 0.05:
            sentiment = 'positive'
        elif sentiment_score['compound'] <= -0.05:
            sentiment = 'negative'
        else:
            sentiment = 'neutral'

        # Save PySpark model to MongoDB
        path_to_model = r'logistic_regression'  # Adjust the path to your pre-trained model
        pipeline_model = PipelineModel.load(path_to_model)

        # Convert the PySpark model to a dictionary for storage in MongoDB
        model_dict = pipeline_model.stages[0].extractParamMap()

        # Combine sentiment analysis results and PySpark model parameters into a single dictionary
        combined_entry = {
            'text': text,
            'negative_percentage': sentiment_score['neg'] * 100,
            'neutral_percentage': sentiment_score['neu'] * 100,
            'positive_percentage': sentiment_score['pos'] * 100,
            'compound': sentiment_score['compound'] * 100,
            'sentiment': sentiment,
        }

        # Store combined entry in MongoDB
        combined_collection.insert_one(combined_entry)

    except json.JSONDecodeError as e:
        print(f"Error decoding JSON: {e}")

# Close down consumer to commit final offsets.
consumer.close()
