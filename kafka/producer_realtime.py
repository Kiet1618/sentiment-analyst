import json
import logging
from flask import Flask, request
from kafka import KafkaProducer
from googleapiclient.errors import HttpError
from googleapiclient.discovery import build

# Kafka setup
producer = KafkaProducer(bootstrap_servers='localhost:9092')
topic_name = 'youtube_comments'

# YouTube API setup
api_key = "AIzaSyCPtSwMcDbIwjSKOuVEvsquW1Sj3WXdR5c"
youtube = build('youtube', 'v3', developerKey=api_key)

# Flask setup
app = Flask(__name__)

# YouTube Webhook URL
webhook_url = '/youtube/webhook'


@app.route(webhook_url, methods=['POST'])
def youtube_webhook():
    if request.method == 'POST':
        data = json.loads(request.data)
        video_id = data['id']
        
        # Process the video details
        video_data = {
            'video_id': video_id,
            'title': data['snippet']['title'],
            'description': data['snippet']['description']
        }
        print(video_data)
        producer.send(topic_name, value=json.dumps(video_data).encode('utf-8'))

        # Fetch comments for the video
        fetch_video_comments(video_id)

    return '', 200


def fetch_video_comments(video_id):
    try:
        # Perform a request to get video comments
        request = youtube.commentThreads().list(
            part='snippet',
            videoId=video_id,
            textFormat='plainText',
            maxResults=10  # You can adjust the number of comments as needed
        )
        response = request.execute()

        # Process the comments and send to Kafka
        for item in response['items']:
            comment_data = {
                'video_id': video_id,
                'comment_id': item['id'],
                'text': item['snippet']['topLevelComment']['snippet']['textDisplay']
            }
            print(comment_data)
            producer.send(topic_name, value=json.dumps(comment_data).encode('utf-8'))

    except HttpError as e:
        logging.error(f'Error during YouTube API request for comments: {e}')


if __name__ == '__main__':
    app.run(debug=True, port=5000)
