import streamlit as st
import mysql.connector
import pandas as pd
from googleapiclient.discovery import build
from datetime import datetime
import time

# Page Configuration
st.set_page_config(
    page_title="YouTube Data Harvesting",
    page_icon="📺",
    layout="wide"
)

# Database Functions
def connect_to_mysql():
    return mysql.connector.connect(
        host="localhost",
        user="admin",
        password="12345",
        database="youtubedata4"
    )

def create_tables():
    conn = connect_to_mysql()
    cursor = conn.cursor()
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS channels (
            channel_id VARCHAR(255) PRIMARY KEY,
            channel_name VARCHAR(255),
            subscriber_count INT,
            video_count INT,
            view_count BIGINT,
            description TEXT
        )
    """)
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS videos (
            video_id VARCHAR(255) PRIMARY KEY,
            channel_id VARCHAR(255),
            title VARCHAR(255),
            published_date DATETIME,
            view_count INT,
            like_count INT,
            comment_count INT,
            duration VARCHAR(50),
            FOREIGN KEY (channel_id) REFERENCES channels(channel_id)
        )
    """)
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS playlists (
            playlist_id VARCHAR(255) PRIMARY KEY,
            channel_id VARCHAR(255),
            title VARCHAR(255),
            description TEXT,
            published_date DATETIME,
            video_count INT,
            FOREIGN KEY (channel_id) REFERENCES channels(channel_id)
        )
    """)
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS comments (
            comment_id VARCHAR(255) PRIMARY KEY,
            video_id VARCHAR(255),
            author_name VARCHAR(255),
            text TEXT,
            published_date DATETIME,
            like_count INT,
            reply_count INT,
            FOREIGN KEY (video_id) REFERENCES videos(video_id)
        )
    """)
    
    conn.commit()
    conn.close()

# YouTube API Functions
def get_channel_stats(youtube, channel_id):
    try:
        request = youtube.channels().list(
            part="snippet,contentDetails,statistics",
            id=channel_id
        )
        response = request.execute()
        
        if not response['items']:
            return None
            
        channel_data = response['items'][0]
        stats = {
            'channel_id': channel_id,
            'channel_name': channel_data['snippet']['title'],
            'subscriber_count': int(channel_data['statistics'].get('subscriberCount', 0)),
            'video_count': int(channel_data['statistics'].get('videoCount', 0)),
            'view_count': int(channel_data['statistics'].get('viewCount', 0)),
            'description': channel_data['snippet']['description'],
            'playlist_id': channel_data['contentDetails']['relatedPlaylists']['uploads']
        }
        return stats
    except Exception as e:
        st.error(f"Error fetching channel stats: {str(e)}")
        return None

def get_playlist_details(youtube, channel_id):
    try:
        playlists = []
        next_page_token = None
        
        while True:
            request = youtube.playlists().list(
                part="snippet,contentDetails",
                channelId=channel_id,
                maxResults=50,
                pageToken=next_page_token
            )
            response = request.execute()
            
            for item in response['items']:
                playlist_data = {
                    'playlist_id': item['id'],
                    'channel_id': channel_id,
                    'title': item['snippet']['title'],
                    'description': item['snippet']['description'],
                    'published_date': item['snippet']['publishedAt'],
                    'video_count': item['contentDetails']['itemCount']
                }
                playlists.append(playlist_data)
            
            next_page_token = response.get('nextPageToken')
            if not next_page_token:
                break
                
        return playlists
    except Exception as e:
        st.error(f"Error fetching playlist details: {str(e)}")
        return []

def get_video_stats(youtube, playlist_id):
    try:
        videos = []
        next_page_token = None
        
        while True:
            request = youtube.playlistItems().list(
                part='snippet',
                playlistId=playlist_id,
                maxResults=50,
                pageToken=next_page_token
            )
            response = request.execute()
            
            video_ids = [item['snippet']['resourceId']['videoId'] for item in response['items']]
            
            video_request = youtube.videos().list(
                part="snippet,contentDetails,statistics",
                id=','.join(video_ids)
            )
            video_response = video_request.execute()
            
            for video in video_response['items']:
                video_data = {
                    'video_id': video['id'],
                    'title': video['snippet']['title'],
                    'published_date': video['snippet']['publishedAt'],
                    'view_count': int(video['statistics'].get('viewCount', 0)),
                    'like_count': int(video['statistics'].get('likeCount', 0)),
                    'comment_count': int(video['statistics'].get('commentCount', 0)),
                    'duration': video['contentDetails']['duration']
                }
                videos.append(video_data)
            
            next_page_token = response.get('nextPageToken')
            if not next_page_token:
                break
            
        return videos
    except Exception as e:
        st.error(f"Error fetching video stats: {str(e)}")
        return []

def get_video_comments(youtube, video_id, max_comments=100):
    try:
        comments = []
        next_page_token = None
        
        while len(comments) < max_comments:
            try:
                request = youtube.commentThreads().list(
                    part="snippet,replies",
                    videoId=video_id,
                    maxResults=min(100, max_comments - len(comments)),
                    pageToken=next_page_token
                )
                response = request.execute()
                
                for item in response['items']:
                    comment = item['snippet']['topLevelComment']['snippet']
                    reply_count = item['snippet']['totalReplyCount']
                    comment_data = {
                        'comment_id': item['id'],
                        'video_id': video_id,
                        'author_name': comment['authorDisplayName'],
                        'text': comment['textDisplay'],
                        'published_date': comment['publishedAt'],
                        'like_count': comment['likeCount'],
                        'reply_count': reply_count
                    }
                    comments.append(comment_data)
                
                next_page_token = response.get('nextPageToken')
                if not next_page_token:
                    break
                    
            except Exception as api_error:
                if 'commentsDisabled' in str(api_error):
                    st.info(f"Comments are disabled for video {video_id}")
                return []
                
        return comments
    except Exception as e:
        st.warning(f"Error fetching comments for video {video_id}: {str(e)}")
        return []

def save_to_mysql(channel_data, videos_data, youtube):
    try:
        conn = connect_to_mysql()
        cursor = conn.cursor()
        
        # Insert channel data
        channel_query = """
            INSERT INTO channels 
            (channel_id, channel_name, subscriber_count, video_count, view_count, description)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
            channel_name = VALUES(channel_name),
            subscriber_count = VALUES(subscriber_count),
            video_count = VALUES(video_count),
            view_count = VALUES(view_count),
            description = VALUES(description)
        """
        channel_values = (
            channel_data['channel_id'],
            channel_data['channel_name'],
            channel_data['subscriber_count'],
            channel_data['video_count'],
            channel_data['view_count'],
            channel_data['description']
        )
        cursor.execute(channel_query, channel_values)
        
        # Insert videos data
        video_query = """
            INSERT INTO videos 
            (video_id, channel_id, title, published_date, view_count, like_count, comment_count, duration)
            VALUES (%s, %s, %s, REPLACE(REPLACE(%s, 'T', ' '), 'Z', ''), %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
            title = VALUES(title),
            published_date = VALUES(published_date),
            view_count = VALUES(view_count),
            like_count = VALUES(like_count),
            comment_count = VALUES(comment_count),
            duration = VALUES(duration)
        """
        for video in videos_data:
            video_values = (
                video['video_id'],
                channel_data['channel_id'],
                video['title'],
                video['published_date'],
                video['view_count'],
                video['like_count'],
                video['comment_count'],
                video['duration']
            )
            cursor.execute(video_query, video_values)
        
        # Get and save playlist data
        playlists_data = get_playlist_details(youtube, channel_data['channel_id'])
        playlist_query = """
            INSERT INTO playlists 
            (playlist_id, channel_id, title, description, published_date, video_count)
            VALUES (%s, %s, %s, %s, REPLACE(REPLACE(%s, 'T', ' '), 'Z', ''), %s)
            ON DUPLICATE KEY UPDATE
            title = VALUES(title),
            description = VALUES(description),
            published_date = VALUES(published_date),
            video_count = VALUES(video_count)
        """
        for playlist in playlists_data:
            playlist_values = (
                playlist['playlist_id'],
                playlist['channel_id'],
                playlist['title'],
                playlist['description'],
                playlist['published_date'],
                playlist['video_count']
            )
            cursor.execute(playlist_query, playlist_values)
        
        # Get and save comments for each video
        comment_query = """
            INSERT INTO comments 
            (comment_id, video_id, author_name, text, published_date, like_count, reply_count)
            VALUES (%s, %s, %s, %s, REPLACE(REPLACE(%s, 'T', ' '), 'Z', ''), %s, %s)
            ON DUPLICATE KEY UPDATE
            author_name = VALUES(author_name),
            text = VALUES(text),
            published_date = VALUES(published_date),
            like_count = VALUES(like_count),
            reply_count = VALUES(reply_count)
        """
        for video in videos_data:
            comments_data = get_video_comments(youtube, video['video_id'])
            for comment in comments_data:
                comment_values = (
                    comment['comment_id'],
                    comment['video_id'],
                    comment['author_name'],
                    comment['text'],
                    comment['published_date'],
                    comment['like_count'],
                    comment['reply_count']
                )
                cursor.execute(comment_query, comment_values)
        
        conn.commit()
        return True
    except Exception as e:
        st.error(f"Error saving to database: {str(e)}")
        return False
    finally:
        conn.close()

def main():
    st.title("📺 YouTube Channel Data Analytics")
    
    # Initialize YouTube API
    api_key = "AIzaSyCkglXpsoXo7QjsLDBAL8mzCfX4YZzpdtg"
    youtube = build('youtube', 'v3', developerKey=api_key)
    
    # Create database tables
    create_tables()
    
    # Input section
    st.subheader("Channel Data Collection")
    channel_id = st.text_input("Enter YouTube Channel ID")
    
    if st.button("Collect and Store Data"):
        if channel_id:
            with st.spinner("Fetching channel data..."):
                channel_data = get_channel_stats(youtube, channel_id)
                if channel_data:
                    videos_data = get_video_stats(youtube, channel_data['playlist_id'])
                    
                    if videos_data:
                        if save_to_mysql(channel_data, videos_data, youtube):
                            st.success("Data collected and stored successfully!")
                        else:
                            st.error("Error saving data to database")
                    else:
                        st.warning("No videos found for this channel")
                else:
                    st.error("Channel not found")
    
    # Analysis section
    # Analysis section
    st.subheader("Data Analysis")
    analysis_query = st.selectbox(
        "Select Analysis",
        [
            "1. Videos and Their Channels",
            "2. Channels with Most Videos",
            "3. Top 10 Most Viewed Videos",
            "4. Comments per Video",
            "5. Most Liked Videos",
            "6. Video Likes Analysis",
            "7. Channel Views Analysis",
            "8. Channels Active in 2022",
            "9. Average Video Duration by Channel",
            "10. Most Commented Videos"
        ]
    )
    
    if st.button("Generate Analysis"):
        conn = connect_to_mysql()
        cursor = conn.cursor()
        
        if analysis_query == "1. Videos and Their Channels":
            st.subheader("Videos and Their Corresponding Channels")
            cursor.execute("""
                SELECT v.title as video_title, c.channel_name
                FROM videos v
                JOIN channels c ON v.channel_id = c.channel_id
                ORDER BY c.channel_name, v.title
            """)
            data = cursor.fetchall()
            df = pd.DataFrame(data, columns=['Video Title', 'Channel Name'])
            st.dataframe(df)
            
        elif analysis_query == "2. Channels with Most Videos":
            st.subheader("Channels Ranked by Number of Videos")
            cursor.execute("""
                SELECT c.channel_name, COUNT(v.video_id) as video_count
                FROM channels c
                LEFT JOIN videos v ON c.channel_id = v.channel_id
                GROUP BY c.channel_id, c.channel_name
                ORDER BY video_count DESC
            """)
            data = cursor.fetchall()
            df = pd.DataFrame(data, columns=['Channel Name', 'Number of Videos'])
            st.dataframe(df)
            
        elif analysis_query == "3. Top 10 Most Viewed Videos":
            st.subheader("Top 10 Most Viewed Videos")
            cursor.execute("""
                SELECT v.title, c.channel_name, v.view_count
                FROM videos v
                JOIN channels c ON v.channel_id = c.channel_id
                ORDER BY v.view_count DESC
                LIMIT 10
            """)
            data = cursor.fetchall()
            df = pd.DataFrame(data, columns=['Video Title', 'Channel Name', 'Views'])
            st.dataframe(df)
            
        elif analysis_query == "4. Comments per Video":
            st.subheader("Number of Comments per Video")
            cursor.execute("""
                SELECT v.title, COUNT(cm.comment_id) as comment_count
                FROM videos v
                LEFT JOIN comments cm ON v.video_id = cm.video_id
                GROUP BY v.video_id, v.title
                ORDER BY comment_count DESC
            """)
            data = cursor.fetchall()
            df = pd.DataFrame(data, columns=['Video Title', 'Number of Comments'])
            st.dataframe(df)
            
        elif analysis_query == "5. Most Liked Videos":
            st.subheader("Videos with Highest Number of Likes")
            cursor.execute("""
                SELECT v.title, c.channel_name, v.like_count
                FROM videos v
                JOIN channels c ON v.channel_id = c.channel_id
                ORDER BY v.like_count DESC
                LIMIT 10
            """)
            data = cursor.fetchall()
            df = pd.DataFrame(data, columns=['Video Title', 'Channel Name', 'Likes'])
            st.dataframe(df)
            
        elif analysis_query == "6. Video Likes Analysis":
            st.subheader("Video Likes Analysis")
            cursor.execute("""
                SELECT v.title, v.like_count
                FROM videos v
                ORDER BY v.like_count DESC
            """)
            data = cursor.fetchall()
            df = pd.DataFrame(data, columns=['Video Title', 'Likes'])
            st.dataframe(df)
            
        elif analysis_query == "7. Channel Views Analysis":
            st.subheader("Total Views per Channel")
            cursor.execute("""
                SELECT c.channel_name, c.view_count
                FROM channels c
                ORDER BY c.view_count DESC
            """)
            data = cursor.fetchall()
            df = pd.DataFrame(data, columns=['Channel Name', 'Total Views'])
            st.dataframe(df)
            
        elif analysis_query == "8. Channels Active in 2022":
            st.subheader("Channels with Videos Published in 2022")
            cursor.execute("""
                SELECT DISTINCT c.channel_name, 
                       COUNT(v.video_id) as videos_in_2022
                FROM channels c
                JOIN videos v ON c.channel_id = v.channel_id
                WHERE YEAR(v.published_date) = 2022
                GROUP BY c.channel_name
                ORDER BY videos_in_2022 DESC
            """)
            data = cursor.fetchall()
            df = pd.DataFrame(data, columns=['Channel Name', 'Videos Published in 2022'])
            st.dataframe(df)
            
        elif analysis_query == "9. Average Video Duration by Channel":
            st.subheader("Average Video Duration per Channel")
            cursor.execute("""
                SELECT 
                    c.channel_name,
                    AVG(
                        CASE
                            WHEN v.duration REGEXP '^PT([0-9]+)M([0-9]+)S$'
                            THEN 
                                CAST(SUBSTRING_INDEX(SUBSTRING_INDEX(v.duration, 'M', 1), 'PT', -1) AS UNSIGNED) * 60 +
                                CAST(SUBSTRING_INDEX(SUBSTRING_INDEX(v.duration, 'S', 1), 'M', -1) AS UNSIGNED)
                            WHEN v.duration REGEXP '^PT([0-9]+)M$'
                            THEN 
                                CAST(SUBSTRING_INDEX(SUBSTRING_INDEX(v.duration, 'M', 1), 'PT', -1) AS UNSIGNED) * 60
                            WHEN v.duration REGEXP '^PT([0-9]+)S$'
                            THEN 
                                CAST(SUBSTRING_INDEX(SUBSTRING_INDEX(v.duration, 'S', 1), 'PT', -1) AS UNSIGNED)
                            ELSE 0
                        END
                    ) as avg_duration_seconds
                FROM channels c
                JOIN videos v ON c.channel_id = v.channel_id
                GROUP BY c.channel_name
                ORDER BY avg_duration_seconds DESC
            """)
            data = cursor.fetchall()
            df = pd.DataFrame(data, columns=['Channel Name', 'Average Duration (seconds)'])
            # Convert seconds to minutes and seconds
            df['Average Duration'] = df['Average Duration (seconds)'].apply(
                lambda x: f"{int(x/60)}:{int(x%60):02d}"
            )
            df = df.drop('Average Duration (seconds)', axis=1)
            st.dataframe(df)
            
        elif analysis_query == "10. Most Commented Videos":
            st.subheader("Videos with Most Comments")
            cursor.execute("""
                SELECT v.title, c.channel_name, v.comment_count
                FROM videos v
                JOIN channels c ON v.channel_id = c.channel_id
                ORDER BY v.comment_count DESC
                LIMIT 10
            """)
            data = cursor.fetchall()
            df = pd.DataFrame(data, columns=['Video Title', 'Channel Name', 'Number of Comments'])
            st.dataframe(df)
        
        conn.close()

if __name__ == "__main__":
    main()