import json
import logging
from kafka import KafkaProducer
import requests
import schedule
import time

# Kafka setup
producer = KafkaProducer(bootstrap_servers='localhost:9092')
topic_name = 'youtube_comments'

# YouTube API key
api_key = "AIzaSyCPtSwMcDbIwjSKOuVEvsquW1Sj3WXdR5c"

def youtube_webhook(video_id):
    # Process the video details
    video_data = {
        'video_id': video_id,
        'title': 'Video Title',  # Replace with actual title retrieval logic
        'description': 'Video Description'  # Replace with actual description retrieval logic
    }
    print(video_data)
    producer.send(topic_name, value=json.dumps(video_data).encode('utf-8'))

    # Fetch the active live chat ID for the video
    live_chat_id = get_active_live_chat_id(video_id)

    # Fetch live chat messages
    if live_chat_id:
        fetch_live_chat_messages(live_chat_id)

def get_active_live_chat_id(video_id):
    try:
        url = f"https://www.googleapis.com/youtube/v3/videos?id={video_id}&part=liveStreamingDetails&key={api_key}"
        response = requests.get(url)
        data = response.json()

        live_chat_id = None
        if 'items' in data and data['items']:
            live_chat_id = data['items'][0]['liveStreamingDetails']['activeLiveChatId']

        return live_chat_id

    except Exception as e:
        logging.error(f'Error fetching active live chat ID: {e}')
        return None

def fetch_live_chat_messages(live_chat_id):
    try:
        url = f"https://www.googleapis.com/youtube/v3/liveChat/messages?part=snippet&liveChatId={live_chat_id}&key={api_key}"
        response = requests.get(url)
        data = response.json()

        # Process live chat messages and send to Kafka
        for item in data.get('items', []):
            message_data = {
                'live_chat_id': live_chat_id,
                'message_id': item['id'],
                'text': item['snippet']['textMessageDetails']['messageText']
            }
            print(message_data)
            producer.send(topic_name, value=json.dumps(message_data).encode('utf-8'))

    except Exception as e:
        logging.error(f'Error fetching live chat messages: {e}')

def scheduled_job(video_id):
    youtube_webhook(video_id)

# Replace 'jfKfPfyJRdk' with the actual video ID you want to use
video_id = 'K1XD86TXUCQ'

# Schedule the job to run every 15 seconds
schedule.every(15).seconds.do(scheduled_job, video_id)

while True:
    schedule.run_pending()
    time.sleep(1)
