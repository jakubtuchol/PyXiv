import urllib
from lxml import etree
import re
import time

def readTillFail(initialToken):
    print "Beginning parsing of data using resumption tokens"
    thisToken = initialToken
    n = 1
    while (thisToken):
        someToken  = fetchData(thisToken)
        if (someToken == -1):
            print "Parsing returned no more values" 
            break
        else:
            print "Parse round " + str(n)
            print "Will be using resumption token %s" % (someToken)
            n += 1
            thisToken = someToken
        time.sleep(30)
        print "Done parsing %i rounds of data" % (n)
    print "Done parsing this round of data"

def fetchData(resToken):
    time.sleep(30)
    print "Fetching data"
    url = 'http://export.arxiv.org/oai2/request?verb=ListRecords&resumptionToken=%s' % (resToken)
    data = urllib.urlopen(url).read()
    saveFile = open('metadata.xml','a')
    saveFile.write(data)
    saveFile.write('\n')
    saveFile.close()
    newToken = re.search('<resumptionToken[^>]*>(.*)</resumptionToken>', data)
    print "Our current token is " + str(newToken)
    if (newToken == None):
        return -1
    else: 
        #Writing the resumption tokens to a CSV file in case of data failure
        tokenFile = open('resumptionTokens.csv','a')
        tokenFile.write(newToken.group(1))
        tokenFile.write('\n')
        tokenFile.close()
        return newToken.group(1)
    
url = 'http://export.arxiv.org/oai2/request?verb=ListRecords&metadataPrefix=arXiv&from=2013-11-03&until=2013-11-19'
initialData = urllib.urlopen(url).read()
saveFile = open('metadata.xml','a')
saveFile.write(initialData)
saveFile.write('\n') 
saveFile.close()
curToken = re.search('<resumptionToken[^>]*>(.*)</resumptionToken>', initialData);
readTillFail(curToken.group(1))
print "Done parsing data"
