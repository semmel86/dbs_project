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
    
getTimelineJson()