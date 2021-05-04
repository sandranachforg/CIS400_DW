#!/usr/bin/env python
# coding: utf-8

# #### ETL - Data Transformation
# 
# 05/03/2021

# #### 1. Import libraries

# In[5]:


import tweepy as tw #to 
import re
import pandas as pd
import preprocessor as p
from preprocessor.api import clean, tokenize, parse
import re, string, unicodedata
import nltk
import datetime as dt
import emoji
from nltk.stem import WordNetLemmatizer
nltk.download('wordnet')
nltk.download('vader_lexicon')
nltk.download('stopwords')
from nltk.sentiment.vader import SentimentIntensityAnalyzer
from nltk.corpus import stopwords
stopwords = nltk.corpus.stopwords.words("english")
from textblob import TextBlob

import requests 
import pandas as pd
import json
from pandas.io.json import json_normalize


# ####  We are interested in the sentiment of the following companies :

# a) Netflix (hashtag: netflix) \
# b) Coca Cola (hashtag: CocaCola) \
# c) Tesla (hashtag: Tesla)\
# d) Nike (hashtag: Nike) \
# e) Apple (hashtag: Apple) 

# #### 2. Create a function to extract data from Alpha Vantage
# 

# In[2]:


#Authors: Hussaan, Sahil, Sandra

def get_company_data(symbol,key):
    
    key = key
    url= "https://www.alphavantage.co/query?"
    parameters = {
            "function": "OVERVIEW",
            "symbol":symbol,
            "apikey":key,
            
    }
    r= requests.get(url,params=parameters)
    data = r.json()
    df = json_normalize(data)   #transforms the data into a dataframe
    df  = df[["Name", "Sector", "Industry"]]   #returns only select columns
    
    return df

dfNFLX=get_company_data("NFLX","5LD0SCCP1XWGHFU4") # apply function to get company data
dfTSLA=get_company_data("TSLA","5LD0SCCP1XWGHFU4")
dfKO=get_company_data("KO","5LD0SCCP1XWGHFU4")
dfSBUX=get_company_data("SBUX","5LD0SCCP1XWGHFU4")
dfNKE=get_company_data("NKE","5LD0SCCP1XWGHFU4")

company_dim = pd.concat([dfNFLX,dfTSLA, dfKO, dfSBUX, dfNKE], axis=0, ignore_index=True) #combine all dataframes into a single one
company_dim["company_id"]=company_dim.index+1; # create a company_id for each company


# #### 3. Create a dataframe containing document_id and source
# We only have two sources for this project, the Twitter API and News API

# In[4]:


#Author: Sandra

d= {'document_id': [1, 2], "source": ["Twitter", "News API"]}
document_dim = pd.DataFrame(data=d)

print (document_dim)


# #### 4. Authorize Account for Twitter

# In[19]:


#Author: Sandra

#To get consumer_key, consumer_secret, access_token, access_token_secret you have to apply for Twitter developer account

consumer_key= ''  
consumer_secret= ''
access_token= ""
access_token_secret= ''
callback_url = 'oob'

auth = tw.OAuthHandler(consumer_key, consumer_secret, callback_url) #creating an OAuthHandler instance
redirect_url= auth.get_authorization_url()

print (redirect_url)

auth.set_access_token(access_token, access_token_secret)
api = tw.API(auth, wait_on_rate_limit=True)

user_pin_input = input ("What's the pin value? ")
api = tw.API (auth)


# #### 5. Create a function to extract data from Twitter API 

# In[1]:


#this function takes 2 parameters and helps us extract the data from twitter
# The "hashtag" is the company name we are analyzing
# The "company_id" is the id we assigned to that particular company in the company dimensions

def get_tweets(hashtag, company_id):
    
    number_of_tweets = 100
    tweets = []
    likes= []
    time= []
    user = []
    author = []
    retweet=[]
    
    # q = pass the hashtags
    
    for i in tw.Cursor(api.search, 
                       q= hashtag, 
                       tweet_mode= 'extended', 
                       lang= "en",
                       since= date_since,
                       until = date_until).items(number_of_tweets):
        tweets.append(i.full_text)
        likes.append(i.favorite_count)
        time.append(i.created_at)
        retweet.append(i.retweet_count) 
      
        
    df= pd.DataFrame({'tweets': tweets, 'likes': likes, 'timestamp': time, 'retweet': retweet})
    
    df["document_id"] = 1
    df["company_id"] = company_id
    
    df= df[["company_id", "timestamp", "document_id", "tweets", "retweet", "likes"]]
    
    df.rename(columns = {"timestamp": "time_id", "tweets": "original_description", 
                        "retweet": "retweet_count", "likes": "like_count"}, inplace= True)

    return df


# #### 6. Create function to get data from News API - can also specify the publisher

# In[2]:


"""
@author: Mohamed Abouregila
get_headlines()
 this function takes company, fromDate, and to date in form of  "yyyy-mm-dd", and returns a dataframe of all 
 headlines posted about this company in the specified period

"""

'''Modified: add additional argument: company_id to differentiate from each other - Sandra Nachforg'''

def get_headlines(company_id, company,fromDate,toDate,key):
    
    url= "http://newsapi.org/v2/everything?"
    parameters = {
            "qInTitle": company,
            "language":"en",
            "from":fromDate,
            "to":toDate,
            "apiKey":key,
    }
    
    response = requests.get(url,params=parameters)

    df = response.json()
    df = pd.DataFrame.from_dict(df)
    df = pd.concat([df.drop(['articles'], axis=1), df['articles'].apply(pd.Series)], axis=1)
    df [["Drop","source"]] = df.source.apply(pd.Series)
    df ["company_id"]  = company_id
    df ["document_id"] = 2
    df ["retweet_count"] = ""
    df ["like_count"] = ""
    
    df = df [["company_id", "publishedAt", "document_id", "title", "retweet_count", "like_count"]]
    
    
    df ["publishedAt"] = pd.to_datetime(df.publishedAt).dt.tz_localize(None)
    
    df.rename(columns = {"publishedAt": "time_id", "title": "original_description"}, inplace = True)
    
    return df


# #### 7. Apply function to extract tweets from Twitter
# Apply the "get_tweets()"-  function
# 
# This function takes 2 arguments: hashtag (e.g. netflix), company_id (based on copmany dimension)

# In[5]:


netflix_twitter = get_tweets("netflix", 1)    #netflix, 1 - company_id, 1- document_id 
tesla_twitter = get_tweets("tsla", 2) 
cocacola_twitter = get_tweets("cocacola", 3)
starbucks_twitter = get_tweets("starbucks", 4)
nike_twitter = get_tweets("nike", 5)


# #### 8. Get headlines for the companies - Apply the get_headlines () - function

# In[6]:


netflix_news = get_headlines (1, "netflix",'2021-04-21','2021-04-22', "" )
tesla_news = get_headlines (2, "tesla", '2021-04-21','2021-04-22', "" )
#cocacola_news = get_headlines (3, "cocacola", '2021-04-21','2021-04-22', "" ) #
starbucks_news = get_headlines (4, "starbucks", '2021-04-21','2021-04-22', "" )
nike_news = get_headlines (5, "nike", '2021-04-21','2021-04-22', "" ) 


# #### 9. Concat all dataframes containing tweets and newspaper headlines 
# (stack on top of each other)

# In[7]:


fact_table = pd.concat([netflix_twitter,tesla_twitter, cocacola_twitter, starbucks_twitter, nike_twitter,
                        netflix_news, tesla_news, starbucks_news, nike_news], axis=0)   #stacks each dataframe on top of each other

fact_table.to_csv("fact_table.csv")  #saves fact_table --> in case we want to save a copy to our hard drive


# In[8]:


fact_table.head()


# #### 10. Create function to clean original_descriptions 
# To get it ready for sentiment analysis
# 

# In[9]:


#We need to do this to do sentiment analysis on the headlines/tweets later

#Author: Sandra 

def cleanup_description(s):
    text = s.lower().split()       # makes all characters lower case
    text = list(filter(lambda x: "http" not in x, text))        # removes http
    text = list(filter(lambda x: not x.startswith("@"), text))   #removes the @
    text = list(map(lambda x: emoji.demojize(x, delimiters=("", ",")).replace("_", " "), text))  # replaces emojis with word
    text = list(map(lambda x: re.sub("[^a-zA-Z ]+", "", x), text)) 
    #only keeps letter from a-z and A-Z
    #text = [word for word in text if not word in stopwords.words()]     #removes stop words
    lemmatizer = WordNetLemmatizer()
    text = list(map(lambda x: lemmatizer.lemmatize(x), text))     #lemmatizes the text
    
    return " ".join(text)


# #### 11. Create a function that calculates the sentiment score for each clean_description

# In[10]:


#Author: Sandra

def get_sentiment_score(df):
    
    df["cleaned_description"] =  df["original_description"].apply(lambda x: cleanup_description(x))
    df[["polarity", "subjectivity"]] = df["cleaned_description"].apply(lambda x: pd.Series(TextBlob(x).sentiment))

    for index, row in df["cleaned_description"].iteritems():
        score = SentimentIntensityAnalyzer().polarity_scores(row)
    
        neg = score["neg"]
        neu = score["neu"]
        pos = score["pos"]
        comp = score["compound"]
    
        if neg > pos:
            df.loc[index, "sentiment"] = "negative"
        elif pos > neg:
            df.loc[index, "sentiment"] = "positive"
        else:
            df.loc[index, "sentiment"] = "neutral"
    
    df.rename(columns = {"timestamp": "time_id", "tweets": "original_description", "cleaned_tweets":"cleaned_description", 
                        "retweet": "retweet_count", "likes": "like_count"}, inplace = True)
    
    df = df[["company_id", "time_id", "document_id", "original_description","cleaned_description", "retweet_count", 
            "like_count", "polarity", "subjectivity", "sentiment"]]
    
    
            
    return df


# #### 12. After we have our finished dataframe, we can apply  the "get_sentiment_score"- function

# In[11]:


fact_table = get_sentiment_score(fact_table)


# #### 13. Lastly - we create time dimension by getting all unique timestamps from fact table and then transform it  
# 

# In[3]:


#Author: Sandra

def get_time_dim(df):
    df_time= df[["time_id"]]
    df_time['date_time'] = pd.to_datetime(df['time_id']).dt.time
    df_time['day']= pd.to_datetime(df['time_id']).dt.day
    df_time['month']= pd.to_datetime(df['time_id']).dt.month
    df_time['year']= pd.to_datetime(df['time_id']).dt.year
    
    df_time.drop_duplicates(inplace = True)
    
    return df_time

## timestamp is the primary key here in SQL
## In fact table it is part of the primary key


# In[13]:


df_time = get_time_dim(fact_table)


# In[14]:


df_time


# #### 14. When data is ready, we load it into the MySQL Database

# In[4]:


get_ipython().system('pip install pymysql')
import pymysql


# In[40]:


#create a connection to local database

connection = pymysql.connect(host= '127.0.0.1',
                            user = 'root', 
                            password = '',
                            db= 'pr_sentiment',
                            cursorclass = pymysql.cursors.DictCursor)

print (connection)


# In[41]:


cursor= connection.cursor() #create a cursor element
connection.commit()   #commit the connection


# #### 14.1 Import data into the dimensions

# In[66]:


#Here we load data from our company_dim (Dataframe) into our database table that we already created

for i,row in company_dim.iterrows():
    
    cursor.execute("INSERT INTO company_dim (company_id, company_name, sector, industry) values (%s, %s, %s, %s)", [row ["company_id"],row["Name"], row["Sector"], row ["Industry"]]);

connection.commit()

#load data from document_dim to table in SQL database
for i, row in document_dim.iterrows():
    cursor.execute("INSERT INTO document_dim (document_id, source) values (%s, %s)", [row["document_id"], row["source"]]);
    
connection.commit()

#Load data from df_time to table in database

for i, row in df_time.iterrows():
    cursor.execute("INSERT INTO time_dim (time_id, date_time, day, month, year) values (%s, %s, %s, %s, %s)", [row["time_id"], row ["date_time"], row["day"], row["month"], row["year"]]);
    
connection.commit()

#load data from fact_table into the fact table in SQL

for i, row in fact_table.iterrows():
    cursor.execute ("INSERT INTO fact_table (company_id, time_id, document_id, original_description, cleaned_description, retweet_count, like_count, polarity, subjectivity, sentiment) values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", [row["company_id"], row["time_id"], row["document_id"], row["original_description"], row ["cleaned_description"], row ["retweet_count"], row["like_count"], row["polarity"], row["subjectivity"], row["sentiment"]]);

connection.commit()  #commit changes
connection.close() # close connection

