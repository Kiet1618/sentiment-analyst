import re
import findspark
from pyspark.ml import PipelineModel
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, udf
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# Import necessary libraries for sentiment analysis using Spark MLlib
from pyspark.ml.feature import Tokenizer
from pyspark.ml.feature import StopWordsRemover
from pyspark.ml.feature import HashingTF
from pyspark.ml.feature import IDF
from pyspark.ml.classification import NaiveBayes
from pyspark.ml import Pipeline

def write_row_in_mongo(df):
    mongo_uri = 'mongodb+srv://bigdatapyspark:bigdatapyspark@bigdata.d648sgf.mongodb.net/'

    df.write.format("mongo").mode("append").option("uri", mongo_uri).save()

def sentiment_analysis(text):
    # Load the pre-trained model
    path_to_model = r'logistic_regression'  # Path to the pre-trained model
    pipeline_model = PipelineModel.load(path_to_model)

    # Create a DataFrame with the text column
    df = spark.createDataFrame([(text,)], ["message"])

    # Make predictions
    prediction = pipeline_model.transform(df)
    
    # Extract the probabilities
    get_positive_prob_udf = udf(lambda v: float(v[2]), DoubleType())
    get_negative_prob_udf = udf(lambda v: float(v[0]), DoubleType())
    get_neutral_prob_udf = udf(lambda v: float(v[1]), DoubleType())

    prediction = prediction \
        .withColumn("positive_probability", get_positive_prob_udf(prediction["probability"])) \
        .withColumn("negative_probability", get_negative_prob_udf(prediction["probability"])) \
        .withColumn("neutral_probability", get_neutral_prob_udf(prediction["probability"]))

    # Determine sentiment based on probabilities
    sentiment_udf = udf(lambda pos, neg, neu: 
                        'positive' if pos >= 0.5 else 
                        'negative' if neg >= 0.5 else 
                        'neutral', StringType())
    
    prediction = prediction \
        .withColumn("sentiment", sentiment_udf(prediction["positive_probability"],
                                               prediction["negative_probability"],
                                               prediction["neutral_probability"]))

    # Select the columns of interest
    prediction = prediction.select("message", "positive_probability", "negative_probability", "neutral_probability", "sentiment")

    return prediction

if __name__ == "__main__":
    findspark.init()

    spark = SparkSession \
        .builder \
        .master("local[*]") \
        .appName("YouTubeSentimentAnalysis") \
        .config("spark.mongodb.input.uri", 'mongodb+srv://bigdatapyspark:bigdatapyspark@bigdata.d648sgf.mongodb.net/') \
        .config("spark.mongodb.output.uri", 'mongodb+srv://bigdatapyspark:bigdatapyspark@bigdata.d648sgf.mongodb.net/') \
        .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2") \
        .getOrCreate()

    sc = spark.sparkContext
    sc.setLogLevel('ERROR')

    schema = StructType([StructField("message", StringType())])

    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "youtube_comments") \
        .option("startingOffsets", "latest") \
        .option("header", "true") \
        .load() \
        .selectExpr("CAST(value AS STRING) as message")

    df = df \
        .withColumn("value", from_json("message", schema))

    # Apply sentiment analysis to the streaming DataFrame
    sentiment_udf = udf(sentiment_analysis, StringType())
    df = df.withColumn("sentiment_result", sentiment_udf(df.message))

    # Load results into MongoDB
    query = df.selectExpr("sentiment_result.*").writeStream.queryName("youtube_sentiments") \
        .foreachBatch(write_row_in_mongo).start()

    query.awaitTermination()
