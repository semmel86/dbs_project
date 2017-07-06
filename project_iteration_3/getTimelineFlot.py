'''
Created on 04.07.2017

@author: semmel
'''
import psycopg2.extras
import json
from builtins import int
from datetime import date
from datetime import datetime
import calendar

try:
    conn = psycopg2.connect("dbname='dbs'  host='178.254.35.26' user='testuser' password='testpass'")
except:
    print ("Cannot Connect!")
        
        
def getDataArray():  

    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    
    try:        
        cur.execute("""SELECT COUNT(hashtag_table.hashtag), DATE(tweet.date) FROM tweet
        JOIN contains ON tweet.tweet_id=contains.tweet_id
        LEFT JOIN hashtag_table on contains.hashtag=hashtag_table.hashtag
        GROUP BY DATE(tweet.date)""")
    except:
        print ("I cannot SELECT anything from contains")
    
    timeline = []
    rows = cur.fetchall()
   
    for row in rows:
        d = row[1]
        d=datetime.combine(d, datetime.min.time())
        #d.timestamp()*1000
        timeStp=calendar.timegm(d.timetuple()) * 1000
        timeline.append([ timeStp,row[0]])
        # flot format
        # [date,sum-value], sorted by date asc
    sortedtimeline=sorted(timeline, key=lambda x: x[0])
    print(str(sortedtimeline))
    # print(json.dumps(timeline))
    file_object  = open("sumOfTweetsPerDay.json", "w")
    file_object.write(str(sortedtimeline))
 
def getHashtagDateArray(hashtag):
        
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    
    try:        
        cur.execute("""SELECT COUNT(hashtag_table.hashtag), DATE(tweet.date) FROM tweet
        JOIN contains ON tweet.tweet_id=contains.tweet_id
        LEFT JOIN hashtag_table on contains.hashtag=hashtag_table.hashtag
        WHERE hashtag_table.hashtag like %s GROUP BY DATE(tweet.date)  ;""",(hashtag,))
    except:
        print ("I cannot SELECT anything from contains")
    
    timeline = []
    rows = cur.fetchall()
   
    for row in rows:
        d = row[1]
        d=datetime.combine(d, datetime.min.time())
        #d.timestamp()*1000
        timeStp=calendar.timegm(d.timetuple()) * 1000
        timeline.append([ timeStp,row[0]])
        # flot format
        # [date,sum-value], sorted by date asc
    sortedtimeline=sorted(timeline, key=lambda x: x[0])
    print(str(sortedtimeline))
    # print(json.dumps(timeline))
    file_object  = open("sumOfTagsPerDay.json", "w")
    file_object.write(str(sortedtimeline))
       
//getDataArray()
getHashtagDateArray("#trump2016")