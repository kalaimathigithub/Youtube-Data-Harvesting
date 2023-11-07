import streamlit as st
import psycopg2
import pymongo
from streamlit_option_menu import option_menu
from googleapiclient.discovery import build
import pandas as pd
api_key = 'AIzaSyBUoH3JE-K6vbJWHF8vLCJGWYsGsIiqJbY'
channel_id = 'UCk7NcgnqCmui1AV7MTXZwOw'
youtube = build('youtube', 'v3', developerKey=api_key)

#### function to to channel details####


def get_channel_stats(youtube, channel_id):

    request = youtube.channels().list(
        part="snippet,contentDetails,statistics",
        id=channel_id)
    response = request.execute()

    data = {'Channel_name': response['items'][0]['snippet']['title'],
            'Subscribers': response['items'][0]['statistics']['subscriberCount'],
            'Views': response['items'][0]['statistics']['viewCount'],
            'Total_videos': response['items'][0]['statistics']['videoCount'],
            'upload_id': response['items'][0]['contentDetails']['relatedPlaylists']['uploads']}

    return data

 ## Function to get playlist from youtube##


# Function to get playlist details #
def get_total_playlists(youtube, channel_id, upload_id):

    request = youtube.playlists().list(
        part="snippet,contentDetails,status",
        channelId=channel_id,
        maxResults=50)
    response = request.execute()

    playlists = []

    for i in range(0, len(response['items'])):
        data = {'playlist_id': response['items'][i]['id'],
                'playlist_name': response['items'][i]['snippet']['title'],
                'channel_id': channel_id,
                'upload_id': upload_id}
        playlists.append(data)

    next_page_token = response.get('nextPageToken')

    thriller = True
    while thriller:
        if next_page_token is None:
            thriller = False
        else:
            request = youtube.playlists().list(
                part="snippet,contentDetails,status",
                channelId=channel_id,
                maxResults=50,
                pageToken=next_page_token)
            response = request.execute()

            for i in range(0, len(response['items'])):
                data = {'playlist_id': response['items'][i]['id'],
                        'playlist_name': response['items'][i]['snippet']['title'],
                        'channel_id': channel_id,
                        'upload_id': upload_id}

                playlists.append(data)
            next_page_token = response.get('nextPageToken')

    return playlists


## Function to get video id##
def get_video_id(youtube, upload_id):

    request = youtube.playlistItems().list(
        part='contentDetails',
        playlistId=upload_id,
        maxResults=50)
    response = request.execute()

    video_id = []

    for i in range(len(response['items'])):
        video_id.append(response['items'][i]['contentDetails']['videoId'])

    next_page_token = response.get('nextPageToken')
    umbrella = True

    while umbrella:
        if next_page_token is None:
            umbrella = False
        else:
            request = youtube.playlistItems().list(
                part='contentDetails',
                playlistId=upload_id,
                maxResults=50,
                pageToken=next_page_token)
            response = request.execute()

            for i in range(len(response['items'])):
                video_id.append(response['items'][i]
                                ['contentDetails']['videoId'])
            next_page_token = response.get('nextPageToken')

    return video_id


# Function to get video details##
def get_video_details(youtube, video_id):
    request = youtube.videos().list(
        part='contentDetails, snippet, statistics',
        id=video_id)
    response = request.execute()
    data = {'video_id': response['items'][0]['id'],
            'publishedat': response['items'][0]['snippet']['publishedAt'],
            'video_name': response['items'][0]['snippet']['title'],
            'video_description': response['items'][0]['snippet']['description'],
            'view_count': response['items'][0]['statistics']['viewCount'],
            'like_count': response['items'][0]['statistics'].get('likeCount', 0),
            'favorite_count': response['items'][0]['statistics'].get('favoriteCount', 0),
            'comment_count': response['items'][0]['statistics'].get('commentCount', 0),
            'upload_id': upload_id
            }

    return data


## to get comment details##
def get_comments_details(youtube, video_id):

    request = youtube.commentThreads().list(
        part='id, snippet',
        videoId=video_id,
        maxResults=100)
    response = request.execute()

    data = {'comment_id': response['items'][0]['id'],
            'comment_text': response['items'][0]['snippet']['topLevelComment']['snippet']['textDisplay'],
            'comment_author': response['items'][0]['snippet']['topLevelComment']['snippet']['authorDisplayName'],
            'video_id': video_id}

    return data


# Data store to mongodb#

def mongodb_collections():
    try:
        kalai = pymongo.MongoClient(
            "mongodb+srv://kalaimathi:kalaimathi@cluster0.dquzjm2.mongodb.net/?retryWrites=true&w=majority")
        db = kalai["temp"]
        collection_names = db.list_collection_names(0)
        for i in collection_names:
            db.drop_collection(i)

    except:
        pass


def mdb_data_channel(channel_name):

    kalai = pymongo.MongoClient(
        "mongodb+srv://kalaimathi:kalaimathi@cluster0.dquzjm2.mongodb.net/?retryWrites=true&w=majority")
    db = kalai["youtube"]
    col = db[channel_name]
    data_1 = []
    for i in col.find({}, {"_id": 0, 'channel_data': 1}):
        data_1.append(i['channel_data'])
    df_channel = pd.DataFrame(data_1)
    return df_channel


def mdb_data_playlist(channel_name):

    kalai = pymongo.MongoClient(
        "mongodb+srv://kalaimathi:kalaimathi@cluster0.dquzjm2.mongodb.net/?retryWrites=true&w=majority")
    db = kalai["youtube"]
    col = db[channel_name]

    data_2 = [] 

    for i in col.find({}, {"_id": 0, 'playlist_data': 1}):
        data_2.extend(i['playlist_data'])
    
    df_playlist = pd.DataFrame(data_2)
    return df_playlist


def mdb_data_videodata(channel_name):

    kalai = pymongo.MongoClient(
        "mongodb+srv://kalaimathi:kalaimathi@cluster0.dquzjm2.mongodb.net/?retryWrites=true&w=majority")
    db = kalai["youtube"]
    col = db[channel_name]

    data_3 = []

    for i in col.find({}, {"_id": 0, 'video_data': 1}):
        data_3.extend(i['video_data'])


        df_video = pd.DataFrame(data_3)
    

    return df_video



def mdb_data_comment(channel_name):
    kalai = pymongo.MongoClient(
        "mongodb+srv://kalaimathi:kalaimathi@cluster0.dquzjm2.mongodb.net/?retryWrites=true&w=majority")
    db = kalai["youtube"]
    col = db[channel_name]

    data_4 = []

    for i in col.find({}, {"_id": 0, 'comment_data': 1}):
        data_4.extend(i['comment_data'])

    df_comment = pd.DataFrame(data_4)
    
    return df_comment


def sql_data(channel_name):
    create_sql_tables()
    last_channel = mdb_data_channel(channel_name)
    last_playlist = mdb_data_playlist(channel_name)
    last_video_data = mdb_data_videodata(channel_name)
    last_comment =  mdb_data_comment(channel_name)


    kalai = psycopg2.connect(host='localhost', port=5432,
                             user='postgres', password='root', database='youtube')
    cursor = kalai.cursor()
    
           
    cursor.executemany(f'''insert into youtube_channels (Channel_name, Subscribers, Views, Total_videos, upload_id)
                        values (%s, %s, %s, %s, %s)''', last_channel.values.tolist())
    

    cursor.executemany(f'''insert into youtube_playlist(playlist_id,playlist_name,channel_id,upload_id) values(%s,%s,%s,%s)''',
                       last_playlist.values.tolist())
    

    cursor.executemany(f'''insert into youtube_video_data(video_id,publishedat,video_name,video_description,view_count,like_count,favorite_count,comment_count,upload_id) values(%s,%s,%s,%s,%s,%s,%s,%s,%s)''',
                       last_video_data.values.tolist())
    

                       
    cursor.executemany(f'''insert into youtube_comment(comment_id,comment_text,comment_author,video_id) 
                       values(%s,%s,%s,%s)''', last_comment.values.tolist())
    
                       
    kalai.commit()
    kalai.close()

def create_sql_tables():
        kalai = psycopg2.connect(host='localhost', port=5432,
                                user='postgres', password='root', database='youtube')
        cursor = kalai.cursor() 

        cursor.execute(f"""create table if not exists youtube_channels(
            Channel_name  varchar(255) primary key,
            Subscribers int,
            Views int, 
            Total_videos int,
            upload_id varchar(255));""")


        cursor.execute(f"""create table if not exists  youtube_playlist(
            playlist_id varchar(255),
            playlist_name varchar(255),
            channel_id varchar(255),
            upload_id varchar(255));""")

        cursor.execute(f"""create table if not exists youtube_video_data(
            video_id varchar(255),
            publishedat varchar(255),
            video_name varchar(255),
            video_description text,
            view_count int,        
            like_count int,
            favorite_count int,
            comment_count int,
            upload_id varchar(255));""")

        cursor.execute(f"""create table if not exists  youtube_comment(
            comment_id varchar(255),
            comment_text text,
            comment_author varchar(255),
            video_id varchar(255));""")

    
        kalai.commit()
        kalai.close()
    

def mdb_collection_channelnames():

    kalai = pymongo.MongoClient("mongodb+srv://kalaimathi:kalaimathi@cluster0.dquzjm2.mongodb.net/?retryWrites=true&w=majority")
    db = kalai["youtube"]
    collection_names = db.list_collection_names()
    return collection_names
    

def sql_data_channel_names():
    kalai = psycopg2.connect(host = 'localhost', 
                            user = 'postgres',
                            password = 'root', 
                            database = 'youtube')
    cursor = kalai.cursor()

    cursor.execute(f'''select channel_name from youtube_channels''')
    s = cursor.fetchall()
    a = []
    for i in s:
        a.append(i[0])
    return a


def select_box_channelnames():



    collection_names = mdb_collection_channelnames()
    a = sql_data_channel_names()

    box = []
    for i in collection_names:
        if i not in a:
            box.append(i)
    return box        

def q1_sql():


    kalai = psycopg2.connect(host='localhost', port=5432,
                                user='postgres', password='root', database='youtube')
    cursor = kalai.cursor()

    cursor.execute(f'''select youtube_video_data.video_name, youtube_channels.channel_name
    FROM youtube_video_data
    inner join youtube_channels ON youtube_video_data.upload_id = youtube_channels.upload_id''')

    q = cursor.fetchall()
    i = [i for i in range(1, len(q) + 1)]
    data = pd.DataFrame(q, columns=['Channel Names', 'Total Videos'], index=i)

    data = data.rename_axis('S.No')

    data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))

    return data
     
def q2_sql():
          kalai = psycopg2.connect(host='localhost', port=5432,
                             user='postgres', password='root', database='youtube')
          cursor = kalai.cursor()
          cursor.execute(f'''SELECT youtube_channels.channel_name, COUNT(youtube_video_data.video_id) AS video_count
          from youtube_channels
          inner join youtube_video_data ON youtube_channels.upload_id = youtube_video_data.upload_id
          group by youtube_channels.channel_name
          order by video_count DESC''')

          q = cursor.fetchall()
          

          i = [i for i in range(1, len(q) + 1)]
          data = pd.DataFrame(q, columns=['Channel Names', 'Total Videos'], index=i)

          data = data.rename_axis('S.No')
          data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))

          return data

def q3_sql():
     
     kalai = psycopg2.connect(host='localhost', port=5432,
                             user='postgres', password='root', database='youtube')
     cursor = kalai.cursor()
     cursor.execute(f'''select youtube_video_data.view_count, youtube_channels.channel_name
     from youtube_video_data
     inner join youtube_channels ON youtube_video_data.upload_id = youtube_channels.upload_id
     order by youtube_video_data.view_count DESC
     limit 10''')
     q = cursor.fetchall()

     i = [i for i in range(1, len(q) + 1)]
     data = pd.DataFrame(q,columns=['video_name', 'view_count'], index=i)

     data = data.rename_axis('S.No')

     data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))

     return data

def q4_sql():

    kalai = psycopg2.connect(host='localhost', port=5432,
                             user='postgres', password='root', database='youtube')
    cursor = kalai.cursor()
    cursor.execute(f'''select youtube_video_data.video_name, COUNT(youtube_comment.comment_id) AS comment_count
    from youtube_video_data
    left join youtube_comment ON youtube_video_data.video_id = youtube_comment.video_id
    group by youtube_video_data.video_name''')

    q = cursor.fetchall()

    i = [i for i in range(1, len(q) + 1)]
    data = pd.DataFrame(q, columns=['Video Names', 'Total Comments'], index=i)

    data = data.rename_axis('S.No')

    data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))

    return data

def q5_sql():

    kalai = psycopg2.connect(host='localhost', port=5432,
                             user='postgres', password='root', database='youtube')
    cursor = kalai.cursor()
    cursor.execute(f'''select youtube_channels.channel_name, youtube_video_data.like_count
    from youtube_video_data
    inner join youtube_channels on youtube_video_data.upload_id = youtube_channels.upload_id
    order by youtube_video_data.like_count DESC
    limit 10''')
    
    q = cursor.fetchall()

    i = [i for i in range(1, len(q) + 1)]
    data = pd.DataFrame(q, columns=['Channel Names', 'like count'], index=i)

    data = data.rename_axis('S.No')

    data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))

    return data

def q6_sql():

    kalai = psycopg2.connect(host='localhost', port=5432,
                             user='postgres', password='root', database='youtube')
    cursor = kalai.cursor()
    cursor.execute(f'''select youtube_video_data.publishedat, youtube_channels.channel_name
          FROM youtube_video_data
          inner join youtube_channels ON youtube_video_data.upload_id = youtube_channels.upload_id''')
    
    q = cursor.fetchall()
    

    i = [i for i in range(1, len(q) + 1)]
    data = pd.DataFrame(q, columns=['publishedat','Channel Names'], index=i)

    data = data.rename_axis('S.No')

    data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))

    return data
     
def q7_sql():

    kalai = psycopg2.connect(host='localhost', port=5432,
                             user='postgres', password='root', database='youtube')
    cursor = kalai.cursor()
    cursor.execute(f'''select youtube_channels.channel_name, SUM(views) AS views
    FROM youtube_channels
    GROUP BY youtube_channels.channel_name
    ORDER BY views DESC''')

    q = cursor.fetchall()

    i = [i for i in range(1, len(q) + 1)]
    data = pd.DataFrame(q, columns=['Channel Names', 'total views'], index=i)

    data = data.rename_axis('S.No')

    data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))

    return data

def q8_sql():

    kalai = psycopg2.connect(host='localhost', port=5432,
                             user='postgres', password='root', database='youtube')
    cursor = kalai.cursor()

    cursor.execute(f'''select youtube_channels.channel_name, youtube_video_data.video_description
    from youtube_video_data
	inner join youtube_channels on youtube_video_data.upload_id = youtube_channels.upload_id
	order by youtube_video_data.video_description desc
	limit 10''')

    q = cursor.fetchall()

    i = [i for i in range(1, len(q) + 1)]
    data = pd.DataFrame(q, columns=['Channel Names', 'video_description'], index=i)

    data = data.rename_axis('S.No')

    data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))

    return data

def q9_sql():

    kalai = psycopg2.connect(host='localhost', port=5432,
                             user='postgres', password='root', database='youtube')
    cursor = kalai.cursor()

    cursor.execute(f'''select youtube_video_data.video_name, COUNT(youtube_video_data.like_count) AS like_count
    from youtube_video_data
    left join youtube_comment ON youtube_video_data.video_id = youtube_comment.video_id
    group by youtube_video_data.video_name''')

    q = cursor.fetchall()

    i = [i for i in range(1, len(q) + 1)]
    data = pd.DataFrame(q, columns=['video name', 'like count'], index=i)

    data = data.rename_axis('S.No')

    data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))

    return data


def q10_sql():

    kalai = psycopg2.connect(host='localhost', port=5432,
                                user='postgres', password='root', database='youtube')
    cursor = kalai.cursor()

    cursor.execute(f'''select youtube_video_data.comment_count, youtube_channels.channel_name
    from youtube_video_data
    inner join youtube_channels ON youtube_video_data.upload_id = youtube_channels.upload_id
    order by youtube_video_data.comment_count DESC
    limit 10''')

    q = cursor.fetchall()

    i = [i for i in range(1, len(q) + 1)]
    data = pd.DataFrame(q, columns=['comment_count', 'channel_name'], index=i)

    data = data.rename_axis('S.No')

    data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))

    return data

def total_qry(): 
        st.subheader('Select the Query below')
        
        Q1 = 'Q1-What are the names of all the videos and their corresponding channels?'
        q2 = 'Q2-Which channels have the most number of videos, and how many videos do they have?'
        q3 = 'Q3-What are the top 10 most viewed videos and their respective channels?'
        q4 = 'Q4-How many comments were made on each video with their corresponding video names?'
        q5 = 'Q5-Which videos have the highest number of likes with their corresponding channel names?'
        q6 = 'Q6- what are the names of all the publishesdat and their corresponding channels?'
        q7 = 'Q7-What is the total number of views for each channel with their corresponding channel names?'
        q8 = 'Q8-What are the names of all the video_description and their corresponding channels?'
        q9 = 'Q9-How many likes were made on each video with their corresponding video names?'
        q10 ='Q10-Which channels have the most number of comments , and how many videos do they have?'

          

st.header('YOUTUBE DATA HARVESTING')


with st.sidebar:
    option = option_menu('', options=['Data extract', 'Data to Mongodb', 'Migrate to SQL', 'SQL Queries'])

if option == 'Data extract':
    # channel_id = 'UCk7NcgnqCmui1AV7MTXZwOw'
    channel_id = st.text_input('channel ID:')
    api_key = 'AIzaSyBUoH3JE-K6vbJWHF8vLCJGWYsGsIiqJbY'
    # api_key = st.text_input('api_key:')
    button = st.button(label='submit')

    if button and channel_id is not None:
        youtube = build('youtube', 'v3', developerKey=api_key)

        channel_data = get_channel_stats(youtube, channel_id)
        upload_id = channel_data['upload_id']
        channel_name = channel_data['Channel_name']

        playlist_data = get_total_playlists(youtube, channel_id, upload_id)
        total_video_id = get_video_id(youtube, upload_id)

        video_data = []
        comment_data = []
        for i in total_video_id:
            v = get_video_details(youtube, i)
            video_data.append(v)

            # a=v['comment_count']
            # if int(a)>0:
            try:
                c = get_comments_details(youtube, i)
                comment_data.append(c)
            except:
                pass

        final_data = {'channel_data': channel_data,
                      'playlist_data': playlist_data,
                      'video_data': video_data,
                      'comment_data': comment_data}

        st.json(final_data)

        # Youtube to mongodb #

        mongodb_collections()

        kalai = pymongo.MongoClient(
            "mongodb+srv://kalaimathi:kalaimathi@cluster0.dquzjm2.mongodb.net/?retryWrites=true&w=majority")
        db = kalai["temp"]
        coll = db[channel_name]
        coll.insert_one(final_data)


elif option == 'Data to Mongodb':
    radio = st.radio(label='Data storing to Mongodb',
                     options=['Select one', 'Yes', 'No'])

    if radio == 'Yes':

        kalai = pymongo.MongoClient(
            "mongodb+srv://kalaimathi:kalaimathi@cluster0.dquzjm2.mongodb.net/?retryWrites=true&w=majority")
        db = kalai["temp"]
        collection_names = db.list_collection_names()
        channel_name = collection_names[0]
        coll = db[channel_name]

        final_data = {}

        for i in coll.find():
            final_data.update(i)

        kalai = pymongo.MongoClient(
            "mongodb+srv://kalaimathi:kalaimathi@cluster0.dquzjm2.mongodb.net/?retryWrites=true&w=majority")
        db = kalai["youtube"]
        coll = db[channel_name]
        coll.insert_one(final_data)

        mongodb_collections()

        st.snow()
        st.success('Data successfully stored into mongodb')

    else:
        radio == 'No'

        st.info('process is skipped')
 
# data to sql

elif option == 'Migrate to SQL':

    create_sql_tables()

    # user select one channel name

    option = select_box_channelnames()

    channel_name = st.selectbox(label="channel_name", options = option)

    button = st.button(label='submit')

    if button:
            sql_data(channel_name)
            st.success("Data Successfully Migrated to SQL Database")
            st.balloons()

      # user select one querries
    
q1 = 'Q1-What are the names of all the videos and their corresponding channels?'
q2 = 'Q2-Which channels have the most number of videos, and how many videos do they have?'
        
q3 = 'Q3-What are the top 10 most viewed videos and their respective channels?'
q4 = 'Q4-How many comments were made on each video with their corresponding video names?'
q5 = 'Q5-Which videos have the highest number of likes with their corresponding channel names?'
q6 = 'Q6- what are the names of all the publishesdat and their corresponding channels?'
q7 = 'Q7-What is the total number of views for each channel with their corresponding channel names?'
q8 = 'Q8-What are the names of all the video_description and their corresponding channels?'
q9 = 'Q9-How many likes were made on each video with their corresponding video names?'
q10 ='Q10-Which channels have the most number of comments , and how many videos do they have?'

          
query_option = st.selectbox(
            '', ['Select One', q1, q2, q3, q4, q5, q6, q7, q8, q9, q10])


        
if query_option == q1:
            st.dataframe(q1_sql())

elif query_option == q2:
            st.dataframe(q2_sql())

elif query_option == q3:
            st.dataframe(q3_sql())

elif query_option == q4:
            st.dataframe(q4_sql())

elif query_option == q5:
            st.dataframe(q5_sql())

elif query_option == q6:
            st.dataframe(q6_sql())

elif query_option == q7:
            st.dataframe(q7_sql())

elif query_option == q8:
            
                st.dataframe(q8_sql())
elif query_option == q9:
            st.dataframe(q9_sql())

elif query_option == q10:
            st.dataframe(q10_sql())
            st.submit(query_option)
            

