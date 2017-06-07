# imports
from bs4 import BeautifulSoup
import requests
import operator

def getHeise(searchedPhrase,limit=10000):
    
    available=True
    offset=0
    allTitles =[]
    allWords=[]
    dict={}
    
    def getResultCount():
            url="https://www.heise.de/suche/?q="+searchedPhrase+"&search_submit.x=0&search_submit.y=0&rm=search&offset=0"
            response=requests.get(url)
            available=response.ok
            if(available):
                soup = BeautifulSoup(response.text, 'html.parser')
                result=soup.find_all('p','search-result-info')
                result=result.pop()
                result=result.get_text()
                result=result.replace('Ergebnisse','')
                n=int(result.strip())
                print(n,"Ergebnisse")
                return n
            
            
   # set a limit
    results=getResultCount()
    if(results<limit):limit=results
    
    while(available==True):
        try:
            url="https://www.heise.de/suche/?q="+searchedPhrase+"&search_submit.x=0&search_submit.y=0&rm=search&offset="+str(offset)
           
            response=requests.get(url)
            available=response.ok
            if(available):
                soup = BeautifulSoup(response.text, 'html.parser')
                for h in soup.find_all('h1'):
                    title=h.get_text()
                    title= title.replace('\n','') # clean
                    title= title.strip() 
                    titleWords=title.split() 
                    allTitles.append(title)
                    allWords=allWords+titleWords
             
            offset+=20
            print("Parse Ergebnisse von",offset-20,"bis",offset)
            if(offset>limit):available=False
        except Exception as e:
            available=False
            print(e)

    
    for n in range(len(allWords)):
        if(dict.__contains__(allWords[n])):
            dict[allWords[n]]+=1
        else:
            dict.__setitem__(allWords[n], 1)
 
           
    sorteddict = sorted(dict.items(), key=operator.itemgetter(1))
    print("Top 1:",sorteddict[-1][0],"\tAnzahl:",sorteddict[-1][1],"\nTop 2:",sorteddict[-2][0],"\tAnzahl:",sorteddict[-2][1],"\nTop 3:",sorteddict[-3][0],"\tAnzahl:",sorteddict[-3][1])
        

# main 
if __name__ == '__main__':
    getHeise("https",)
