import json
import uuid
from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from googleapiclient.discovery import build
from clickhouse_driver import Client

# Параметры аутентификации YouTube API
api_key = 'OUR_API_KEY'
youtube = build('youtube', 'v3', developerKey=api_key)

# Параметры подключения к ClickHouse
clickhouse_host = 'OUR_CLICKHOUSE_HOST'
clickhouse_port = 'OUR_CLICKHOUSE_PORT'
clickhouse_user = 'OUR_CLICKHOUSE_USERNAME'
clickhouse_password = 'OUR_CLICKHOUSE_PASSWORD'
clickhouse_database = 'OUR_CLICKHOUSE_DATABASE'

default_args = {
    'owner': 'I_AM',
    'start_date': datetime(2023, 9, 22),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'youtube_video_search',
    default_args=default_args,
    schedule_interval='@daily',
)

def get_youtube_videos():
    videos = []
    nextPageToken = None

    while True:
        request = youtube.search().list(
            q='"Power BI" OR "Power Query"',  # Поисковой запрос
            part='snippet',
            maxResults=20,
            pageToken=nextPageToken
        )
        response = request.execute()
        videos.extend(response.get('items', []))

        nextPageToken = response.get('nextPageToken')
        if not nextPageToken:
            break

    return videos

def filter_videos(videos):
    filtered_videos = []
    for video in videos:
        channel_id = video['snippet']['channelId']
        request = youtube.channels().list(
            part='statistics',
            id=channel_id
        )
        video_response = youtube.videos().list(part='contentDetails', id=video['id']['videoId']).execute()
        response = request.execute()
        subscriber_count = int(response['items'][0]['statistics']['subscriberCount'])
        if subscriber_count >= 1000:
            filtered_videos.append({
                'title': video['snippet']['title'],
                'publish_date': video['snippet']['publishedAt'],
                'subscriber_count': subscriber_count,
                'channel_title': video['snippet']['channelTitle'],
                'video_length': video_response['items'][0]['contentDetails']['duration'],
                'channel_id': channel_id,
                'video_id': video['id']['videoId'],
            })
    return filtered_videos

def save_to_clickhouse(videos):
    client = Client(host=clickhouse_host, port=clickhouse_port, user=clickhouse_user, password=clickhouse_password,
                    database=clickhouse_database)
    query_channel = 'INSERT INTO channel (channel_id, subscribers, channel_title) VALUES (?, ?, ?)'
    query_date = 'INSERT INTO date (date_id, publish_date) VALUES (?, ?)'
    query_query = 'INSERT INTO query (query_id, query) VALUES (?, ?)'
    query_video = 'INSERT INTO video (video_id, channel_id, date_id, duration, query_id) VALUES (?, ?, ?, ?, ?)'

    for video in videos:
        video_id = video['video_id']
        existing_video_query = f'SELECT video_id FROM video WHERE video_id = \'{video_id}\''
        existing_video = client.execute(existing_video_query)
        if existing_video:
            continue

        channel_data = (uuid.uuid4(), video['subscriber_count'], video['channel_title'])
        date_data = (uuid.uuid4(), video['publish_date'])
        query_data = (uuid.uuid4(), video['query'])
        video_data = (video_id, video['channel_id'], date_data[0], video['video_length'], query_data[0])

        client.execute(query_channel, [channel_data])
        client.execute(query_date, [date_data])
        client.execute(query_query, [query_data])
        client.execute(query_video, [video_data])

# def save_to_file(videos):
#     with open('result.json', 'w') as file:
#         json.dump(videos, file)

def search_and_save_videos():
    videos = get_youtube_videos()
    filtered_videos = filter_videos(videos)
    save_to_clickhouse(filtered_videos)
    # save_to_file(filtered_videos)

task_search_save_videos = PythonOperator(
    task_id='search_save_videos',
    python_callable=search_and_save_videos,
    dag=dag,
)

task_search_save_videos