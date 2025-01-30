from datetime import datetime

import pandas
import pyspark.sql.functions
from googleapiclient.discovery import build
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, explode, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, ArrayType, LongType

import main.config as config
import re
from azure_utils import set_azure_configuration

spark = (SparkSession.builder.appName("clean data").config(
    "spark.jars.packages",
    "org.apache.hadoop:hadoop-azure:3.3.1,com.microsoft.azure:azure-storage:8.6.6")
         .master('local[*]').getOrCreate())
spark.sparkContext.setLogLevel("DEBUG")


def fetch_youtube_data(**kwargs):
    api_key = config.api_key
    youtube = build('youtube', 'v3', developerKey=api_key)
    video_data = []
    regions = kwargs['country_codes']
    for region in regions:
        next_page_token = None
        counter = 0
        while True:
            request = youtube.videos().list(
                fields='nextPageToken,items(id,snippet(title,description,tags,categoryId,channelId,channelTitle,publishedAt),contentDetails,statistics)',
                part='snippet,contentDetails,statistics',
                chart="mostPopular",
                regionCode=region,
                maxResults=100,
                pageToken=next_page_token
            )
            response = request.execute()
            videos = response['items']
            for i, video in enumerate(videos):
                video_info = {
                    'rank': counter + i + 1,
                    'region_code': region,
                    'category_id': video['snippet']['categoryId'],
                    'video_id': video['id'],
                    'title': video['snippet']['title'],
                    'channel_id': video['snippet']['channelId'],
                    'tags': video['snippet'].get('tags', []),
                    'channel_title': video['snippet']['channelTitle'],
                    'duration': video['contentDetails']['duration'],
                    'published_at': video['snippet']['publishedAt'],
                    'view_count': video['statistics'].get('viewCount', 0),
                    'like_count': video['statistics'].get('likeCount', 0),
                    'comment_count': video['statistics'].get('commentCount', 0),
                }
                video_data.append(video_info)
            counter += 50
            next_page_token = response.get('nextPageToken')
            if not next_page_token:
                break
    df = pandas.DataFrame(video_data)
    current_date = datetime.now().strftime("%d%m%Y")
    file_path = config.output_filepath + 'extract_' + current_date + '.csv'
    df.to_csv(file_path, index=False, encoding="utf-8-sig")

def remove_emoji(value):
    emoji = re.compile("["
                       u"\U0001F600-\U0001F64F"
                       u"\U0001F300-\U0001F5FF"
                       u"\U0001F680-\U0001F6FF"
                       u"\U0001F1E0-\U0001F1FF"
                       u"\U00002500-\U00002BEF"
                       u"\U00002702-\U000027B0"
                       u"\U00002702-\U000027B0"
                       u"\U000024C2-\U0001F251"
                       u"\U0001f926-\U0001f937"
                       u"\U00010000-\U0010ffff"
                       u"\u2640-\u2642"
                       u"\u2600-\u2B55"
                       u"\u200d"
                       u"\u23cf"
                       u"\u23e9"
                       u"\u231a"
                       u"\ufe0f"
                       u"\u3030"
                       "]+", re.UNICODE)
    return re.sub(emoji, '', value)


def process_data():
    current_date = datetime.now().strftime("%d%m%Y")
    input_path = config.output_filepath + 'extract_' + current_date + '.csv'

    df_schema = StructType([
        StructField('rank', IntegerType()),
        StructField('region_code', StringType()),
        StructField('category_id', IntegerType()),
        StructField('video_id', StringType()),
        StructField('title', StringType()),
        StructField('channel_id', StringType()),
        StructField('tags', StringType()),
        StructField('channel_title', StringType()),
        StructField('duration', StringType()),
        StructField('published_at', TimestampType()),
        StructField('view_count', LongType()),
        StructField('like_count', LongType()),
        StructField('comment_count', LongType())
    ])
    raw_df = spark.read.schema(df_schema).option("header", True).csv(input_path)
    df_filtered = raw_df.withColumn('tags', pyspark.sql.functions.trim('tags')).filter(raw_df.tags != '[]')
    df_corrected = (df_filtered.withColumn('tags', pyspark.sql.functions.from_json('tags', ArrayType(StringType())))
                    .withColumn('published_date', to_date('published_at')))

    remove_emoji_udf = pyspark.sql.functions.udf(remove_emoji, StringType())
    df2 = df_filtered.withColumn('title', remove_emoji_udf(col('title')))
    df2.show()
    df_exploded = df_corrected.withColumn('tags_ex', explode('tags')).drop('tags')
    # current_date = datetime.now().strftime("%d%m%Y")
    # file_path = config.output_filepath + '/output/' + current_date
    # df_exploded.write.mode('overwrite').parquet(file_path)
    return df_exploded

def upload_data_to_storage():
    set_azure_configuration(spark, config.azure_storage_acc, config.azure_client_id, config.azure_client_secret,
                            config.azure_tenant_id)
    df = process_data()
    current_date = datetime.now().strftime("%d%m%Y")
    results_path = config.output_filepath + '/output/' + current_date
    write_path = "abfss://" + config.azure_container_name + "@" + config.azure_storage_acc + ".dfs.core.windows.net/" + results_path
    df.write.mode("overwrite").format("parquet").save(write_path)