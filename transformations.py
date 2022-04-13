import pandas as pd
import numpy as np
# import matplotlib.pyplot as plt
import datetime as dt

#Author: Ahad Syed
#Data Engineer Assessment


#-----------------------------------------------------------------------------#
#                           Question 1: Alogrithm                             # 
#-----------------------------------------------------------------------------#

df = pd.read_csv('Data Engineer Take home dataset - dataset.csv')  

#Remove rows with one or more duplicates, keeps first occurence by default
df = df.drop_duplicates()

#Remove any rows where user_id is null or any timestamps are null 
df = df.dropna(subset=['user_id', 'timestamp'])


#Remove rows with dirty flyer_id as the data is corrupted 
#Remove non-float merchant_id and flyer_id values
df.drop(df[df['flyer_id'] == '%3Cnull%3G'].index, inplace = True)
df.drop(df[df['merchant_id'] == '<null>'].index, inplace = True)

#convert timestamp field to datetime object, it's str in original dataset, check type: print(type(df.timestamp[0]))
df['timestamp'] = pd.to_datetime(df['timestamp'])


#Check how many days worth of data we have, we see that only one day of data is available
# print(min(df['timestamp']))
# print(max(df['timestamp']))


#Check that event types: list_flyers, shopping_list_open and favorite don't have a flyer_id/merchant_id
df_check_data = df[(df["event"] == 'list_flyers') | (df["event"] == 'shopping_list_open') | (df["event"] == 'favorite')]
# print(df_check_data)


#Here we make an assumption that to have an item open you must be inside a flyer.
#Thus we also include the item_open times along with the flyer open times in our calculations.
#A potential issue with getting the view_time of an event with the max/min timestamp approach after a groupby
#is that there are several events where both the max and min are the same resulting in a 0 view_time.
#This can happen if there is an event of a user viewing a flyer only once, which seems to be quite common.
#We also assume that in between the max and min timestamps for a given user and flyer, this user doesn't
#view other flyers in that time period.

df_filtered = df.loc[(df['event'] == 'flyer_open') | (df['event'] == 'item_open')] 
df_grouped = df_filtered.groupby(['user_id', 'flyer_id', 'merchant_id']).agg({'timestamp': ['max', 'min']})
df_grouped.columns = ['max_timestamp', 'min_timestamp'] #rename columns
df_grouped = df_grouped.reset_index() 
df_grouped['view_time'] = (df_grouped['max_timestamp'] - df_grouped['min_timestamp'])
# print(df_grouped.head(50))

#=========================================
# **ANOTHER APPROACH** would be to sort timestamp values by acsending then group by user_id.
#This creates an event history for each user. Then make the assumption that the end timestamp 
#of a given event is just the 'timestamp' of the next event. Use diff() to calculate view_time.
#This approach seems to give better metrics, but would need to have a discussion with 
#data owners to decide which approach is more accurate to fit the data. 

df = df.sort_values(by='timestamp')
df['view_time'] = df.groupby('user_id')['timestamp'].diff() 
df['view_time'] = df['view_time'].shift(-1)
# print(df.head(50))


#Calculate avg time on flyer per user
df_avg_time_on_flyer_per_user = df_grouped.groupby('user_id')['view_time'].mean()
print(df_avg_time_on_flyer_per_user.head(50))

# df_avg_time_on_flyer_per_user_Approach2 = df.groupby('user_id')['view_time'].mean()
# print(df_avg_time_on_flyer_per_user_Approach2.head(50))


#-----------------------------------------------------------------------------#
#                  Question 2: Metrics to back BI Report                      #
#-----------------------------------------------------------------------------#


#Average time spent on a given merchants flyers/items
df_avg_time_on_flyer_per_merchant = df_grouped.groupby('merchant_id')['view_time'].mean()
print(df_avg_time_on_flyer_per_merchant)


#Total time spent on a given merchants flyer/items
df_total_time_on_flyer_per_merchant = df_grouped.groupby('merchant_id')['view_time'].sum()
print(df_total_time_on_flyer_per_merchant)


#Total number of times a given merchant has been favorited
df_fav = df.loc[(df['event'] == 'favorite')] 
df_total_fav_per_merchant = df_fav.groupby('merchant_id')['event'].count()
print(df_total_fav_per_merchant)


#how many distinct users open each merchants flyers per day
df_unique_users_per_merchant = df.groupby('merchant_id')['user_id'].nunique()
print(df_unique_users_per_merchant)


#How many times user views a given merchant's flyer until they mark them as a favorite
#This becomes a more useful metric to calculate if we have multiple days of data 


#-----------------------------------------------------------------------------#
#                  Question 3: Scalability -- In Q3.pdf attached              #
#-----------------------------------------------------------------------------#