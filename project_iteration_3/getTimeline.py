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
def getTimelineJson():
    try:
        conn = psycopg2.connect("dbname='dbs'  host='178.254.35.26' user='testuser' password='testpass'")
    except:
        print ("Cannot Connect!")
    
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    
    try:
        cur.execute("""SELECT DISTINCT hashtag_table.hashtag, tweet.date FROM tweet
    JOIN contains ON tweet.tweet_id=contains.tweet_id
    LEFT JOIN hashtag_table on contains.hashtag=hashtag_table.hashtag""")
        
    except:
        print ("I cannot SELECT anything from contains")
    
    timeline = []
    rows = cur.fetchall()
    i=1
    for row in rows:
        # timeline format
        #{id: 1, content: 'item 1', start: '2013-04-20'}
        #{id: 1, content: 'item 1', start: '2013-04-20'}
        timeline.append({"id":i,"content":row[0],"start":str(row[1])})
        i+=1
        
    print(json.dumps(timeline))
    file_object  = open("timeline.json", "w")
    file_object.write(json.dumps(timeline))
    
def getDataArray():  
    try:
        conn = psycopg2.connect("dbname='dbs'  host='178.254.35.26' user='testuser' password='testpass'")
    except:
        print ("Cannot Connect!")
        
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
    
getDataArray()
getTimelineJson()