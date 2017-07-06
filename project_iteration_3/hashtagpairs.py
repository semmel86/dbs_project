import random
import json
import psycopg2.extras

# Try to connect
try:
    conn = psycopg2.connect("dbname='dbs'  host='178.254.35.26' user='testuser' password='testpass'")
except:
    print ("Cannot Connect!")

# get cursor and proceed query
cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

try:
    # Count == Node size
    cur.execute("""SELECT hashtag,COUNT(hashtag) FROM contains GROUP BY hashtag""")
except:
    print ("I cannot SELECT anything from contains")

hashtags = []
nodesDict={}# {hastag:count}
# print results
rows = cur.fetchall()
i=1
for row in rows:
    hashtags.append(row[0].lower())
    nodesDict[row[0].lower()]=(row[1],i,row[0].lower())
    i+=1
    
try:   
    cur.execute("""Select c2.hashtag, contains.hashtag FROM contains 
    Join contains as c2 ON contains.hashtag<>c2.hashtag AND c2.tweet_id=contains.tweet_id ORDER BY c2.hashtag""")

except:
    print ("I cannot SELECT anything from contains")
hashtagpairs = []
rows = cur.fetchall() 
for row in rows:   
    hashtagpairs.append((row[0],row[1]))
print(str(hashtagpairs))    


def getColor():
        # set cluster color
        r=random.randint(25, 255)
        g=random.randint(25, 255)
        b=random.randint(25, 255)
        color ="rgb("+str(r)+","+str(g)+","+str(b)+")"
        return color
# use dictionary for the Nodes and Edges

def getNode(hashtag,color):
    # TODO add meaningful coordinates
    x=random.randint(0, 1400)
    y=random.randint(0, 1000)
    #x=koordX(hashtag)
    #y=koordY(hashtag)
    node={"id":nodesDict[hashtag][1],"label":hashtag,"x":x,"y":y,"size":(2000*nodesDict[hashtag][0]),"color":color}
    return node

def getEdge(id,src,tar,color="rgb(199,199,199)"):
    edge={"id":id,"source":src,"target":tar,"color":color}
    return edge

def fillNodes():
    nodes=[]
    for i in range(len(hashtags)): 
        nodes.append(getNode(hashtags[i],getColor()))
    return nodes      
def fillEdges():
        #    edgeId= getEdgesForNode(hashtag,2,edgeId)
        edges=[]
        for k in range(len(hashtagpairs)):     
            edges.append(getEdge(k,nodesDict[hashtagpairs[k][0]][1],nodesDict[hashtagpairs[k][1]][1]))
        return edges
allNodes=fillNodes()
allEdges=fillEdges()
print(str(allEdges))       
# write to file
file_object  = open("C:/xampp/htdocs/SigmaJS/data2.json", "w")
file_object.write("{\"nodes\":" + json.dumps(allNodes)+",\"edges\":"+json.dumps(allEdges)+"}");
