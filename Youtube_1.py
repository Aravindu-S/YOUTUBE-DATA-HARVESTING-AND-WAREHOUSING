# Connecting Youtube API

import googleapiclient.discovery
import pymongo
import psycopg2
import pandas as pd
import streamlit as st 


api_service_name = "youtube"
api_version = "v3" 

youtube = googleapiclient.discovery.build(
        api_service_name, api_version, developerKey="AIzaSyDiWvJE3tZpR3YwDtBEa_tzb0mjq0QAe9U")


# To pull channel data

def get_channel_data(channel_id):

    request = youtube.channels().list(
            part="snippet,contentDetails,statistics",
            id=channel_id
        )
    response_channel = request.execute()

    channel_data = dict(
        Channel_Name = response_channel['items'][0]['snippet']['title'],
        Channel_Ids = response_channel['items'][0]['id'],
        Subscription_Count = response_channel['items'][0]['statistics']['subscriberCount'],
        Channel_Views = response_channel['items'][0]['statistics']['viewCount'],
        Channel_Description = response_channel['items'][0]['snippet']['description'],
        Total_videos = response_channel['items'][0]['statistics']['videoCount'],
        Playlist_Id = response_channel['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    )

    return channel_data

#  To pull video_ids

def get_video_ids(channel_id):

    video_ids=[]
    next_page = None
    request = youtube.channels().list(
            part="contentDetails",
            id=channel_id
        )
    response_channel = request.execute()

    Playlist_Id = response_channel['items'][0]['contentDetails']['relatedPlaylists']['uploads']


    while True:
        request = youtube.playlistItems().list(
                part="contentDetails",
                maxResults=50,
                playlistId=Playlist_Id,
                pageToken = next_page
        )
        response_video = request.execute()

        for i in range(len(response_video['items'])):
            video_ids.append(response_video['items'][i]['contentDetails']['videoId'])
    
        if "nextPageToken" in response_video:
            next_page = response_video["nextPageToken"]
        else:
            break  
    return video_ids

# To pull individual video details

def get_video_details(videos_ids):
    video_details = []

    for i in videos_ids:
        request = youtube.videos().list(
                part="snippet,contentDetails,statistics",
                id= i
            )
        response_VideoDetails= request.execute()

        vi_details= dict(
                Channel_Name = response_VideoDetails['items'][0]['snippet']['channelTitle'],
                Channel_Ids= response_VideoDetails['items'][0]['snippet']['channelId'],
                Video_id = response_VideoDetails['items'][0]['id'],
                video_name = response_VideoDetails['items'][0]['snippet']['title'],
                video_description = response_VideoDetails['items'][0]['snippet']['description'],
                published_date = response_VideoDetails['items'][0]['snippet']['publishedAt'],
                view_count = response_VideoDetails['items'][0]['statistics']['viewCount'],
                like_count= response_VideoDetails['items'][0]['statistics'].get('likeCount'),
                favorite_count= response_VideoDetails['items'][0]['statistics'].get('favoriteCount'),
                comment_count= response_VideoDetails['items'][0]['statistics'].get('commentCount'), 
                duration= response_VideoDetails['items'][0]['contentDetails']['duration'],
                thumbnail= response_VideoDetails['items'][0]['snippet']['thumbnails']['default']['url'],
                caption_status= response_VideoDetails['items'][0]['contentDetails']['caption']

            )
        video_details.append(vi_details)
    return video_details

# To pull individual comment data

def get_comment_data(videos_ids):
    comment_datas=[]
    for video in videos_ids:
        try:
            next_page= None
            while True:
                    request = youtube.commentThreads().list(
                            part="snippet",
                            videoId= video,
                            maxResults= 100,
                            pageToken= next_page
                        )
                    response_commentdetails = request.execute()

                        
                    for i in response_commentdetails['items']:
                        co_data= dict(
                        Comment_Id= i['id'],
                        Video_id=i['snippet']['videoId'],
                        Comment_Text= i['snippet']['topLevelComment']['snippet']['textDisplay'],
                        Comment_Author= i['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                        Comment_PublishedAt= i['snippet']['topLevelComment']['snippet']['publishedAt']  
                        )
                        comment_datas.append(co_data)
                    
                    if "nextPageToken" in response_commentdetails:
                        next_page = response_commentdetails["nextPageToken"]
                    else:
                        break 
          
        except:
            pass
    return comment_datas

# Uploading data to Mongo DB

client = pymongo.MongoClient("mongodb://localhost:27017/")
db1 = client["YouTube"]

def channel_details(Channel_Id):

    channel_detail= get_channel_data(Channel_Id)
    videos_ids= get_video_ids(Channel_Id)
    Video_details= get_video_details(videos_ids)   
    Comment_details= get_comment_data(videos_ids)

    col1= db1["Channel Details"]
    col1.insert_one({'Channel Info': channel_detail,'Video Info': Video_details, 'Comment info': Comment_details })

    return "Channel details has been uploaded into Mongo DB"


# T0 create Channel table in SQL, fetch data from MongoDB and insert data into SQL

# Creating table in SQL

db_1=psycopg2.connect(host='localhost',
                    user='postgres',
                    password='@122Madras',
                    database='Youtube',
                    port= '5432')

cursor = db_1.cursor()

# Creating channel table in postgres

create_query='''CREATE TABLE IF NOT EXISTS channels(Channel_Name varchar(100),
                                                Channel_Ids varchar(100) primary key,
                                                Subscription_Count bigint,
                                                Channel_Views bigint,
                                                Channel_Description text,
                                                Total_videos int,
                                                Playlist_Id varchar(100))'''
cursor.execute(create_query)
db_1.commit()


# Creating videos table in postgres  


create_query='''CREATE TABLE IF NOT EXISTS videos(Channel_Name  varchar(100),
                                                Channel_Ids varchar(50),
                                                Video_id  varchar(50) primary key,
                                                video_name  varchar(100),
                                                video_description text ,
                                                published_date timestamp,
                                                view_count bigint,
                                                like_count bigint,
                                                favorite_count int,
                                                comment_count bigint, 
                                                duration interval,
                                                thumbnail varchar(100),
                                                caption_status varchar(50)
                                                )'''
cursor.execute(create_query)
db_1.commit()

# Creating comment table in postgres

create_query='''CREATE TABLE IF NOT EXISTS comments(Comment_Id varchar(200) primary key,
                                                    Video_id varchar(100),
                                                    Comment_Text text,
                                                    Comment_Author varchar(150),
                                                    Comment_PublishedAt timestamp  
                                                    )'''
cursor.execute(create_query)
db_1.commit()

def channel_table(Channel_Name):

# SQL Connection

    db_1=psycopg2.connect(host='localhost',
                        user='postgres',
                        password='@122Madras',
                        database='Youtube',
                        port= '5432')

    cursor = db_1.cursor()

# Fetching channel data from MongoDB 
    channel_data=[]
    col1= db1["Channel Details"]

    for cha_data in col1.find({},{'_id':0,'Channel Info':1}):
        if cha_data['Channel Info']['Channel_Name'] == Channel_Name:
            channel_data.append(cha_data['Channel Info'])

    df1=pd.DataFrame(channel_data)

    


# Inserting channel data into table
        
    for index,row in df1.iterrows():
        insert_query= '''INSERT INTO channels (Channel_Name,
                                            Channel_Ids,
                                            Subscription_Count,
                                            Channel_Views,
                                            Channel_Description,
                                            Total_videos,
                                            Playlist_Id)

                                            VALUES(%s,%s,%s,%s,%s,%s,%s)
                            ON CONFLICT (Channel_Ids) DO NOTHING'''
        
        value=(row['Channel_Name'],
            row['Channel_Ids'],
            row['Subscription_Count'],
            row['Channel_Views'],
            row['Channel_Description'],
            row['Total_videos'],
            row['Playlist_Id'])
    
        cursor.execute(insert_query,value)
        db_1.commit()

# T0 create Videos table in SQL, fetch data from MongoDB and insert data into SQL
        

def video_table(Channel_Name):

#SQL Connection     
  
    db_1=psycopg2.connect(host='localhost',
                        user='postgres',
                        password='@122Madras',
                        database='Youtube',
                        port= '5432')

    cursor = db_1.cursor()
        

    # Fetching videos data from MongoDB
    videos_data=[]
    col1= db1["Channel Details"]

    for vid_data in col1.find({},{'_id':0,'Channel Info':1,'Video Info':1}):
        if vid_data['Channel Info']['Channel_Name'] == Channel_Name:
            for i in vid_data['Video Info']:
                videos_data.append(i)

    df2=pd.DataFrame(videos_data)


    # Inserting videos table in postgres
    for index,row in df2.iterrows():
        insert_query= '''INSERT INTO videos(Channel_Name,
                                            Channel_Ids,
                                            Video_id,
                                            video_name,
                                            video_description,
                                            published_date,
                                            view_count,
                                            like_count,
                                            favorite_count,
                                            comment_count, 
                                            duration,
                                            thumbnail,
                                            caption_status)
                                            
                                            VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                         ON CONFLICT (Video_id) DO NOTHING'''
        
        value=(row['Channel_Name'],
            row['Channel_Ids'],
            row['Video_id'],
            row['video_name'],
            row['video_description'],
            row['published_date'],
            row['view_count'],
            row['like_count'],
            row['favorite_count'],
            row['comment_count'],
            row['duration'],
            row['thumbnail'],
            row['caption_status'])
        
        
      
        cursor.execute(insert_query,value)
        db_1.commit()


# T0 create Comments table in SQL, fetch data from MongoDB and insert data into SQL

def comment_table(Channel_Name):

#SQL Connection 

    db_1=psycopg2.connect(host='localhost',
                        user='postgres',
                        password='@122Madras',
                        database='Youtube',
                        port= '5432')

    cursor = db_1.cursor()

    # Fetching comment data from MongoDB
    comment_data=[]
    col1= db1["Channel Details"]

    for com_data in col1.find({},{'_id':0,'Channel Info':1,'Comment info':1}):
        if com_data['Channel Info']['Channel_Name'] == Channel_Name:
            for i in com_data['Comment info']:
                comment_data.append(i)

    df3=pd.DataFrame(comment_data)


    for index,row in df3.iterrows():
        insert_query= '''INSERT INTO comments(Comment_Id,
                                            Video_id,
                                            Comment_Text,
                                            Comment_Author,
                                            Comment_PublishedAt)
                                            
                                            VALUES(%s,%s,%s,%s,%s)
                            ON CONFLICT (Comment_Id) DO NOTHING'''
        
        value=(row['Comment_Id'],
            row['Video_id'],
            row['Comment_Text'],
            row['Comment_Author'],
            row['Comment_PublishedAt']
            )
    
        
    
        cursor.execute(insert_query,value)
        db_1.commit()


def tables(Channel_Name):
    channel_table(Channel_Name)
    video_table(Channel_Name)
    comment_table(Channel_Name)

    return ('Tables created in SQL successfully')


def show_channel_details():
    
    channel_data=[]
    col1= db1["Channel Details"]

    for cha_data in col1.find({},{'_id':0,'Channel Info':1}):
        channel_data.append(cha_data['Channel Info'])

    df1=st.dataframe(channel_data)

def show_video_details():    
    
    videos_data=[]
    col1= db1["Channel Details"]

    for vid_data in col1.find({},{'_id':0,'Video Info':1}):
        for i in vid_data['Video Info']:
            videos_data.append(i)

    df2=st.dataframe(videos_data)

def show_comment_details():
    
    comment_data=[]
    col1= db1["Channel Details"]

    for com_data in col1.find({},{'_id':0,'Comment info':1}):
        for i in com_data['Comment info']:
            comment_data.append(i)

    df3=st.dataframe(comment_data)

def cha_name():
    channel_name=[]
    col1= db1["Channel Details"]

    for cha_data in col1.find({},{'_id':0,'Channel Info':1}):
        name=cha_data['Channel Info']["Channel_Name"]
        channel_name.append(name)
    return (channel_name)



#STREAMLIT
st.set_page_config(
    page_title='YOUTUBE DATA HARVESTING AND WAREHOUSING PROJECT',
    layout="wide"
    )
    

st.title(':red[YOUTUBE DATA HARVESTING AND WAREHOUSING PROJECT]')

with st.sidebar:
    st.title(':blue[This Streamlit application allows users to access and analyze data from multiple YouTube channels]')
    st.header('The application has the following features:')
    st.markdown('- Ability to input a YouTube channel ID and retrieve all the relevant data using Google API')
    st.markdown('- Option to store the data in a MongoDB database as a data lake')
    st.markdown('- Ability to collect data from YouTube channels and store them in the data lake by clicking a button')
    st.markdown('- Option to select a channel name and migrate its data from the data lake to a SQL database as tables')
    st.markdown('- Ability to search and retrieve data from the SQL database using different search options')
    st.header('Upcoming features:')
    st.markdown('- Visualization of data using graphs')
    st.text("")
    st.text("")
    st.link_button("LinkedIn", "https://www.linkedin.com/in/aravindusivashankar/")
    st.link_button("GitHub", "https://github.com/Aravindu-S")


channel_id=st.text_input("Enter the channel ID: ")


if st.button("Click to fetch and store channel details"):

    channel_data=[]
    col1= db1["Channel Details"]

    for cha_data in col1.find({},{'_id':0,'Channel Info':1}):
        channel_data.append(cha_data['Channel Info']['Channel_Ids'])

    if channel_id in channel_data:
        st.error('Channel details already exists in the database')

    else:
        insert=channel_details(channel_id)
        st.success(insert)
        
channel_names=cha_name() #To get the channel names of the channel that are uploaded in MongoDB
name = st.selectbox("Select the channel name to migrate to SQL",(channel_names))

if st.button("Migrate the data to SQL"):
    Table= tables(name)
    st.success(Table)

show_table=st.radio('Select the table to display', ('CHANNELS','VIDEOS','COMMENTS'), horizontal=True)

if show_table == 'CHANNELS':
    show_channel_details()

elif show_table == 'VIDEOS':
    show_video_details()

elif show_table == 'COMMENTS':
    show_comment_details()


# SQL Connection to fetch query

db_1=psycopg2.connect(host='localhost',
                    user='postgres',
                    password='@122Madras',
                    database='Youtube',
                    port= '5432')

cursor = db_1.cursor()

questions=st.selectbox("Select your questions",
                       ('1. All the videos and their corresponding channels',
                        '2. Channels with the most number of videos',
                        '3. The top 10 most viewed videos',
                        '4. Comment count of each videos',
                        '5. Video with highest likes',
                        '6. Like count of each videos',
                        '7. Views count of each videos',
                        '8. The names of all the channels that have published videos in the year 2023',
                        '9. The average duration of all videos in each channel',
                        '10. Videos that have highest number of comments')) 

if questions == '1. All the videos and their corresponding channels':
    query1= '''SELECT channel_name as Channel_Name, video_name as Video_Name FROM videos'''
    cursor.execute(query1)
    db_1.commit()

    table_1= cursor.fetchall()
    df=pd.DataFrame(table_1,columns=['Channel Name','Video Name'])

    st.write(df)

elif questions == '2. Channels with the most number of videos':
    query2= '''SELECT channel_name,total_videos FROM channels 
               ORDER BY total_videos DESC'''
    
    cursor.execute(query2)
    db_1.commit()

    table_1= cursor.fetchall()
    df=pd.DataFrame(table_1,columns=['Channel Name','Total Videos'])

    st.write(df)

elif questions == '3. The top 10 most viewed videos':
    query3= '''SELECT channel_name, video_name,view_count FROM videos 
               ORDER BY view_count DESC 
               LIMIT 10'''
    
    cursor.execute(query3)
    db_1.commit()

    table_1= cursor.fetchall()
    df=pd.DataFrame(table_1,columns=['Channel Name','Video Name','Total Views'])

    st.write(df)

elif questions == '4. Comment count of each videos':
    query4= '''SELECT channel_name, video_name,comment_count FROM videos
               ORDER BY comment_count DESC'''
    
    cursor.execute(query4)
    db_1.commit()

    table_1= cursor.fetchall()
    df=pd.DataFrame(table_1,columns=['Channel Name','Video Name','Total Comment'])

    st.write(df)

elif questions == '5. Video with highest likes':
    query5= '''SELECT channel_name,video_name, like_count FROM videos
               ORDER BY like_count DESC
               LIMIT 10'''
    
    cursor.execute(query5)
    db_1.commit()

    table_1= cursor.fetchall()
    df=pd.DataFrame(table_1,columns=['Channel Name','Video Name','Total Likes'])

    st.write(df)

elif questions == '6. Like count of each videos':
    query6= '''SELECT video_name, like_count FROM videos
               ORDER BY like_count DESC'''
    
    cursor.execute(query6)
    db_1.commit()

    table_1= cursor.fetchall()
    df=pd.DataFrame(table_1,columns=['Video Name','Total Likes'])

    st.write(df)

elif questions == '7. Views count of each videos':
    query7= '''SELECT video_name, view_count FROM videos
               ORDER BY view_count DESC'''
    
    cursor.execute(query7)
    db_1.commit()

    table_1= cursor.fetchall()
    df=pd.DataFrame(table_1,columns=['Video Name','Total Views'])

    st.write(df)


elif questions == '8. The names of all the channels that have published videos in the year 2023':
    query8= '''SELECT channel_name,video_name,published_date FROM videos
               WHERE EXTRACT (YEAR FROM published_date)='2023' '''
    
    cursor.execute(query8)
    db_1.commit()

    table_1= cursor.fetchall()
    df=pd.DataFrame(table_1,columns=['Channel Name','Video Name','Total Views'])

    st.write(df)

elif questions == '9. The average duration of all videos in each channel':
    query9= '''SELECT channel_name,AVG(duration) as average FROM videos
           GROUP BY channel_name
           ORDER BY average DESC'''
    
    cursor.execute(query9)
    db_1.commit()

    table_1= cursor.fetchall()
    df=pd.DataFrame(table_1,columns=['Channel Name','Average Duration'])

    temp=[]

    for index,row in df.iterrows():
        name = row['Channel Name']
        Duration= (str(row['Average Duration']))
        temp.append(dict(Channel_name= name,Average_Duration= Duration))

    df_new=pd.DataFrame(temp)
    df_new.rename(columns={'Channel_name':'Channel Name','Average_Duration':'Average Duration'},inplace=True)

    st.write(df_new)

elif questions == '10. Videos that have highest number of comments':
    query10= '''SELECT video_name, comment_count FROM videos
                ORDER BY comment_count DESC
                LIMIT 10'''
    
    cursor.execute(query10)
    db_1.commit()

    table_1= cursor.fetchall()
    df=pd.DataFrame(table_1,columns=['Video Name','Total Comments'])

    st.write(df)