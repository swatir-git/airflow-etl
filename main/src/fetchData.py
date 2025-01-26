import pandas
from Tools.scripts.dutree import display
from googleapiclient.discovery import build
from unicodedata import category
from pandas import *


def fetch_youtube_data(api_key, regions):
    youtube = build('youtube', 'v3', developerKey=api_key)
    video_data = []
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
                'rank': i+1,
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
    return pandas.DataFrame(video_data)


api_key = 'AIzaSyCKeTJ871OrOQ7SgB53IL_l-37E3dDE82I'
regions = ['IN','GB','US']
df = fetch_youtube_data(api_key, regions)
df.to_csv('C:\\Users\\Swati_Rallabandi\\Desktop\\PySparkProjects\\airflow-output\\output.csv',index = False, encoding="utf-8-sig")
