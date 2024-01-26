import json
import logging
from kafka import KafkaConsumer
from pymongo import MongoClient
from pyspark.ml import PipelineModel
from pyspark.sql import SparkSession

# MongoDB setup
mongo_uri = "mongodb+srv://bigdatapyspark:bigdatapyspark@bigdata.d648sgf.mongodb.net/"
database_name = "bigdata"
collection_name = "Comment"

# Spark setup
spark = SparkSession.builder.appName("CommentAnalysis").getOrCreate()

# Connect to MongoDB
client = MongoClient(mongo_uri)
db = client[database_name]
collection = db[collection_name]

# Kafka setup
consumer = KafkaConsumer('youtube_comments', bootstrap_servers='localhost:9092', group_id='comment_analysis_group')

# Load Spark ML model
model_path = r"logistic_regression"
loaded_model = PipelineModel.load(model_path)

def analyze_comment(comment_text, model):
    # Replace with your actual comment analysis logic
    # For example, if using Spark ML's transform method on the loaded model
    result = model.transform(spark.createDataFrame([(comment_text,)]))
    return result.collect()[0]['prediction']  # Assuming 'prediction' is the relevant output column

def store_in_mongodb(data):
    try:
        # Insert the data into MongoDB
        collection.insert_one(data)
        print(f"Data stored in MongoDB: {data}")

    except Exception as e:
        logging.error(f'Error storing data in MongoDB: {e}')

def consume_analyze_and_store(model):
    for message in consumer:
        try:
            # Decode the message value from Kafka
            message_data = json.loads(message.value.decode('utf-8'))

            # Check if the message contains a live chat message
            if 'live_chat_id' in message_data and 'text' in message_data:
                # Analyze the comment
                analysis_result = analyze_comment(message_data['text'], model)

                # Store the analysis result in MongoDB
                store_in_mongodb({
                    'live_chat_id': message_data['live_chat_id'],
                    'comment_text': message_data['text'],
                    'analysis_result': analysis_result
                })

        except Exception as e:
            logging.error(f'Error processing and storing message: {e}')

if __name__ == "__main__":
    # Start consuming, analyzing, and storing messages
    consume_analyze_and_store(loaded_model)
