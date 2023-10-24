import streamlit as st
from googleapiclient.discovery import build
import pymongo
import mysql.connector as sql
import pandas as pd

st.markdown("<h1 style='text-align: center; color: white;'>Youtube Data Harvesting</h1>", unsafe_allow_html=True)

api_key = 'AIzaSyAk0aKdF6Gc9iGdkMpy00qVscBtza0b96Y'
youtube = build('youtube','v3',developerKey=api_key)

def channel(channel_id):
    response = youtube.channels().list(part="snippet,contentDetails,statistics", id=channel_id).execute()

    data = {
        'Channel_id': response['items'][0]['id'],
        'Channel_name': response['items'][0]['snippet']['title'],
        'Channel_views': int(response['items'][0]['statistics']['viewCount']),
        'Channel_description': response['items'][0]['snippet']['description'][:200],
        'Published_year': int(response['items'][0]['snippet']['publishedAt'][:4])
    }

    return data

def playlist(channel_id):
    response = youtube.playlists().list(part="snippet,contentDetails", channelId=channel_id, maxResults=25).execute()

    playlists = []
    for item in response['items']:
        data = {
            'Playlist_id': item['id'],
            'Channel_id': item['snippet']['channelId'],
            'Playlist_name': item['snippet']['title'][:50]
        }
        playlists.append(data)

    return playlists

def comment(video_id):
    response = youtube.commentThreads().list(part="snippet,replies", maxResults=25, videoId=video_id).execute()

    comments = []
    for item in response['items']:
        data = {
            'Comment_id': item['id'],
            'Video_id': item['snippet']['videoId'],
            'Comment_text': item['snippet']['topLevelComment']['snippet']['textDisplay'][:100],
            'Comment_author': item['snippet']['topLevelComment']['snippet']['authorDisplayName']
        }
        comments.append(data)

    return comments

def video(channel_id):

    videos = []
    
    for p in playlist(channel_id):
        playlist_id = p['Playlist_id']
        playlist_response = youtube.playlistItems().list(part="contentDetails,id", maxResults=25, playlistId=playlist_id).execute()
        for item in playlist_response['items']:
            video_id = item['contentDetails']['videoId']
            video_response = request = youtube.videos().list(part="snippet,contentDetails,statistics", id=video_id).execute()
            
            data = {
                'Video_id': video_id,
                'Playlist_id': playlist_id,
                'Video_name': video_response['items'][0]['snippet']['title'],
                'Video_description': video_response['items'][0]['snippet']['description'][:200],
                'Published_year': int(video_response['items'][0]['snippet']['publishedAt'][:4]),
                'View_count': int(video_response['items'][0]['statistics']['viewCount']),
                'Like_count': int(video_response['items'][0]['statistics']['likeCount']),
                'Favorite_count': int(video_response['items'][0]['statistics']['favoriteCount']),
                'Comment_count': int(video_response['items'][0]['statistics']['commentCount']),
                'Duration': duration_change(video_response['items'][0]['contentDetails']['duration'])
            }
            videos.append(data)
    return videos

def duration_change(duration):
    duration_in_seconds = 0
    value = ''
    for c in duration[2:]:
        if c == 'H':
            duration_in_seconds += (int(value)*3600)
            value = ''
        elif c == 'M':
            duration_in_seconds += (int(value)*60)
            value = ''
        elif c == 'S':
            duration_in_seconds += int(value)
            value = ''
        else:
            value += c
    return duration_in_seconds

mydb = sql.connect(host="localhost", user="root", password="12345", database= "youtube_db", port = "3306")
mycursor = mydb.cursor()

def sql_insert_channels(data):
    insert_query = "INSERT INTO channel (Channel_id, Channel_name, Channel_views, Channel_description, Published_year) VALUES (%s, %s, %s, %s, %s)"
    values = (data['Channel_id'], data['Channel_name'], data['Channel_views'], data['Channel_description'], data['Published_year'])

    mycursor.execute(insert_query, values)
    mydb.commit()

def sql_insert_playlists(data_list):
    for data in data_list:
        insert_query = "INSERT INTO playlist (Playlist_id, Channel_id, Playlist_name) VALUES (%s, %s, %s)"
        values = (data['Playlist_id'], data['Channel_id'], data['Playlist_name'])

        mycursor.execute(insert_query, values)
        mydb.commit()

def sql_insert_videos(data_list):
    for data in data_list:
        insert_query = "INSERT INTO video (Video_id, Playlist_id, Video_name, Video_description, Published_year, View_count, Like_count, Favorite_count, Comment_count, Duration) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        values = (data['Video_id'], data['Playlist_id'], data['Video_name'], data['Video_description'], data['Published_year'], data['View_count'], data['Like_count'], data['Favorite_count'],data['Comment_count'] , data['Duration'])

        mycursor.execute(insert_query, values)
        mydb.commit()

def sql_insert_comments(data_list):
    for data in data_list:
            insert_query = "INSERT INTO comment (Comment_id, Video_id, Comment_text, Comment_author) VALUES (%s, %s, %s, %s)"
            values = (data['Comment_id'], data['Video_id'], data['Comment_text'], data['Comment_author'])

            mycursor.execute(insert_query, values)
            mydb.commit()

channel_id = st.text_input("Enter Channel ID")
button_clicked = st.button('Add')

client = pymongo.MongoClient('mongodb://localhost:27017')
db = client['Youtube_Database']

if (button_clicked) and (channel_id is not None):
    with st.spinner('Please Wait...'):
        #data dictionary
        channel_details = channel(channel_id)
        playlist_details = playlist(channel_id)
        video_details = video(channel_id)
        
        comment_list = []
        for v in video_details:
            video_id = v['Video_id']
            comment_list += comment(video_id)

        # MongoDB
        collections1 = db['Channel']
        collections2 = db['Playlist']
        collections3 = db['Video']
        collections4 = db['Comment']

        collections1.insert_one(channel_details)
        collections2.insert_many(playlist_details)
        collections3.insert_many(video_details)
        collections4.insert_many(comment_list)

        #SQL
        sql_insert_channels(channel_details)
        sql_insert_playlists(playlist_details)
        sql_insert_videos(video_details)
        sql_insert_comments(comment_list)

        st.success("Done!")
    
question = st.selectbox('Select the question',
    ['1. What are the names of all the videos and their corresponding channels?',
    '2. Which channels have the most number of videos, and how many videos do they have?',
    '3. What are the top 10 most viewed videos and their respective channels?',
    '4. How many comments were made on each video, and what are their corresponding video names?',
    '5. Which videos have the highest number of likes, and what are their corresponding channel names?',
    '6. What is the total number of likes for each video, and what are their corresponding video names?',
    '7. What is the total number of views for each channel, and what are their corresponding channel names?',
    '8. What are the names of all the channels that have published videos in the year 2022?',
    '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?',
    '10. Which videos have the highest number of comments, and what are their corresponding channel names?'])

if question == '1. What are the names of all the videos and their corresponding channels?':
    query = mycursor.execute(
                             """SELECT v.Video_name, c.Channel_name
                                FROM video v
                                JOIN playlist p ON v.Playlist_id = p.Playlist_id
                                JOIN channel c ON p.Channel_id = c.Channel_id;"""
                            )
    df = pd.DataFrame(mycursor.fetchall(), columns=mycursor.column_names)
    st.write(df)

elif question == '2. Which channels have the most number of videos, and how many videos do they have?':
    query = mycursor.execute(
                             """SELECT c.Channel_name, COUNT(v.Video_id) AS VideoCount
                                FROM channel c
                                JOIN playlist p ON c.Channel_id = p.Channel_id
                                JOIN video v ON p.Playlist_id = v.Playlist_id
                                GROUP BY c.Channel_name
                                ORDER BY VideoCount DESC
                                LIMIT 1;"""
                            )
    df = pd.DataFrame(mycursor.fetchall(), columns=mycursor.column_names)
    st.write(df)

elif question == '3. What are the top 10 most viewed videos and their respective channels?':
    query = mycursor.execute(
                             """SELECT v.Video_name, c.Channel_name, v.View_count
                                FROM video v
                                JOIN playlist p ON v.Playlist_id = p.Playlist_id
                                JOIN channel c ON p.Channel_id = c.Channel_id
                                ORDER BY v.View_count DESC
                                LIMIT 10;"""
                            )
    df = pd.DataFrame(mycursor.fetchall(), columns=mycursor.column_names)
    st.write(df)

elif question == '4. How many comments were made on each video, and what are their corresponding video names?':
    query = mycursor.execute(
                             """SELECT v.Video_name, v.Comment_count
                                FROM video v;"""
                            )
    df = pd.DataFrame(mycursor.fetchall(), columns=mycursor.column_names)
    st.write(df)

elif question == '5. Which videos have the highest number of likes, and what are their corresponding channel names?':
    query = mycursor.execute(
                             """SELECT v.Video_name, v.Like_count, c.Channel_name
                                FROM video v
                                JOIN playlist p ON v.Playlist_id = p.Playlist_id
                                JOIN channel c ON p.Channel_id = c.Channel_id
                                ORDER BY v.Like_count DESC
                                LIMIT 1;"""
                            )
    df = pd.DataFrame(mycursor.fetchall(), columns=mycursor.column_names)
    st.write(df)

elif question == '6. What is the total number of likes for each video, and what are their corresponding video names?':
    query = mycursor.execute(
                             """SELECT v.Video_name, v.Like_count
                                FROM video v;"""
                            )
    df = pd.DataFrame(mycursor.fetchall(), columns=mycursor.column_names)
    st.write(df)

elif question == '7. What is the total number of views for each channel, and what are their corresponding channel names?':
    query = mycursor.execute(
                             """SELECT c.Channel_name, SUM(v.View_count) AS TotalViews
                                FROM channel c
                                JOIN playlist p ON c.Channel_id = p.Channel_id
                                JOIN video v ON p.Playlist_id = v.Playlist_id
                                GROUP BY c.Channel_name;"""
                            )
    df = pd.DataFrame(mycursor.fetchall(), columns=mycursor.column_names)
    st.write(df)

elif question == '8. What are the names of all the channels that have published videos in the year 2022?':
    query = mycursor.execute(
                             """SELECT DISTINCT c.Channel_name
                                FROM channel c
                                JOIN playlist p ON c.Channel_id = p.Channel_id
                                JOIN video v ON p.Playlist_id = v.Playlist_id
                                WHERE v.Published_year = 2022;"""
                            )
    df = pd.DataFrame(mycursor.fetchall(), columns=mycursor.column_names)
    st.write(df)

elif question == '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?':
    query = mycursor.execute(
                             """SELECT c.Channel_name, AVG(v.Duration) AS AverageDuration
                                FROM channel c
                                JOIN playlist p ON c.Channel_id = p.Channel_id
                                JOIN video v ON p.Playlist_id = v.Playlist_id
                                GROUP BY c.Channel_name;"""
                            )
    df = pd.DataFrame(mycursor.fetchall(), columns=mycursor.column_names)
    st.write(df)

elif question == '10. Which videos have the highest number of comments, and what are their corresponding channel names?':
    query = mycursor.execute(
                             """SELECT v.Video_name, v.Comment_count, c.Channel_name
                                FROM video v
                                JOIN playlist p ON v.Playlist_id = p.Playlist_id
                                JOIN channel c ON p.Channel_id = c.Channel_id
                                ORDER BY v.Comment_count DESC
                                LIMIT 1;"""
    )
    df = pd.DataFrame(mycursor.fetchall(), columns=mycursor.column_names)
    st.write(df)