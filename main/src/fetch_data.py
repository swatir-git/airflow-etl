from datetime import datetime

import pandas
from googleapiclient.discovery import build
import main.config as config


def fetch_youtube_data(**kwargs):
    api_key = config.api_key
    youtube = build('youtube', 'v3', developerKey=api_key)
    video_data = []
    regions = kwargs['country_codes']
    for region in regions:
        request = youtube.videos().list(
            part='snippet,contentDetails,statistics',
            chart="mostPopular",
            regionCode=region,
            maxResults=50
        )
        response = request.execute()
        videos = response['items']
        for i, video in enumerate(videos):
            video_info = {
                'rank': i + 1,
                'region_code': region,
                'category_id': video['snippet']['categoryId'],
                'video_id': video['id'],
                'title': video['snippet']['title'],
                'channel_id': video['snippet']['channelId'],
                'channel_title': video['snippet']['channelTitle'],
                'duration': video['contentDetails']['duration'],
                'published_at': video['snippet']['publishedAt'],
                'view_count': video['statistics'].get('viewCount', 0),
                'like_count': video['statistics'].get('likeCount', 0),
                'comment_count': video['statistics'].get('commentCount', 0)
            }
            video_data.append(video_info)
    df = pandas.DataFrame(video_data)
    current_date = datetime.now().strftime("%d-%m-%Y-%H-%M")
    file_path = config.output_filepath + current_date + '.csv'
    df.to_csv(file_path, index=False, encoding="utf-8-sig")
