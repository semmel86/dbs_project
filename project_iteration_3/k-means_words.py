import random
import json
import psycopg2.extras
import distance
import math

def hamming_dist(x,y):
    d = 0
    for i in range(len(x)):
        if x[i]!=y[i]:
            d+=1
    return d

def midpoint(cluster):
    if cluster:
        maximum_distance = [(max(dist(hashtag, h) for h in cluster)) for hashtag in cluster]
        minimum_distance = min(maximum_distance)
        index =  maximum_distance.index(minimum_distance)
        return cluster[index]
    else:
        return ''


def dist(x,y):
    #return hamming_dist(x,y)
    return distance.levenshtein(x, y)

def koordX(string):
    result=0
    for c in string:
        result+=ord(c)
    result+=random.randint(-250*math.floor(math.log(result)),250*math.floor(math.log(result)))
    return result

def koordY(string):
    l=len(string)
    y=l*100+random.randint(-200,200)
    return y

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
Join contains as c2 ON contains.hashtag<>c2.hashtag AND c2.tweet_id=contains.tweet_id""")

except:
    print ("I cannot SELECT anything from contains")
hashtagpairs = []
rows = cur.fetchall() 
for row in rows:   
    hashtagpairs.append((row[0],row[1]))
print(str(hashtagpairs))    
# define maximum number of clusters
number_of_clusters = 5
center_new=[]
center=[]
center_dist = [0]*len(center)


# generate centers of clusters at random
i = 0
while i<number_of_clusters:
    word = hashtags[random.randint(0, len(hashtags)-1)]
    if word not in center_new:
        center_new.append(word)
        center.append('a')
        i+=1
print("init random center: "+str(center_new))


while max(dist(center[i],center_new[i]) for i in range(len(center)))>0:

    # flush cluster
    cluster= [[] for i in range(len(center))]
    # copy previous centerpoints
    center= [center_new[i] for i in range(len(center))]
    # assign cluster to hashtags

    for tag in hashtags:
    # find closest centerpoint
        min_center = center[0]
        index = 0
        for i in range(len(center)):
            if dist(tag,center[i])<dist(tag,min_center):
                min_center = center[i]
                index = i

        # assign hashtag to cluster
        cluster[index].append(tag)
       


    # find new centerpoints for clusters
    for i in range(len(center)):
        center_new[i]=midpoint(cluster[i])
    print("current center: "+str(center_new))

for i in range(len(cluster)):
    print("cluster "+str(i)+": "+str(cluster[i]))
 

allEdges=[]
# use dictionary for the Nodes and Edges
def getNode(hashtag,color):
    # TODO add meaningful coordinates
    #x=random.randint(0, 500)
    #y=random.randint(0, 500)
    x=koordX(hashtag)
    y=koordY(hashtag)
    node={"id":nodesDict[hashtag][1],"label":hashtag,"x":x,"y":y,"size":(2000*nodesDict[hashtag][0]),"color":color}
    return node
def getEdge(id,src,tar):
    edge={"id":id,"source":src,"target":tar}
    return edge
def getEdgesForNode(hashtag,num,edgeId):


    
    for k in range(len(hashtagpairs)):
        allEdges.append(getEdge(k,nodesDict[hashtagpairs[k][0]][1],nodesDict[hashtagpairs[k][1]][1]))
      
    print(str(allEdges))
    #edges=[]#[(id,distance),..]
    #for node in nodesDict:
    #    edges.append((nodesDict[node][1],dist(nodesDict[node][2],hashtag)))
        
    #sort by distance
    #sortedNodes=sorted(edges, key=lambda x: x[1])
    #for i in range(num):
    #     allEdges.append(getEdge(edgeId,nodesDict[hashtag][1],sortedNodes[i+1][0],))
    #     edgeId+=1
    return 0

# iterate over the cluster
id=1
allNodes=[]
edgeId=0
for i in range(len(cluster)):
        currCluster=cluster[i]
        
        # set cluster color
        r=random.randint(25, 255)
        g=random.randint(25, 255)
        b=random.randint(25, 255)
        color ="rgb("+str(r)+","+str(g)+","+str(b)+")"
        
        print("color "+color)
        # add node to All Nodes
        for j in range(len(currCluster)): 
            hashtag=currCluster[j];
            allNodes.append(getNode(hashtag,color))
            
            #add Edges to Edges
        #    edgeId= getEdgesForNode(hashtag,2,edgeId)
edgeId= getEdgesForNode(hashtag,2,edgeId)
# write to file
file_object  = open("C:/xampp/htdocs/SigmaJS/data.json", "w")
file_object.write("{\"nodes\":" + json.dumps(allNodes)+",\"edges\":"+json.dumps(allEdges)+"}");
