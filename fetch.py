import urllib
from lxml import etree
import re
import time

url = 'http://export.arxiv.org/oai2/request?verb=ListRecords&metadataPrefix=arXiv&from=2013-11-03&until=2013-11-19'
initialData = urllib.urlopen(url).read()
saveFile = open('metadata.xml','a')
saveFile.write(initialData)
saveFile.write('\n') 
saveFile.close()
curToken = re.search('<resumptionToken[^>]*>(.*)</resumptionToken>', initialData);
print curToken.group(1)
time.sleep(30)
url2 = 'http://export.arxiv.org/oai2/request?verb=ListRecords&resumptionToken=%s' % (curToken.group(1))
secondaryData = urllib.urlopen(url).read()
saveFile2 = open('metadata.xml','a')
saveFile2.write(secondaryData)
saveFile2.write('\n') 
saveFile2.close()
print re.search('<resumptionToken[^>]*>(.*)</resumptionToken>', secondaryData);
#root = etree.parse(initialData)
#curToken = root.xpath('//xsi:OAI-PMH/ListRecords/resumptionToken')

#print curToken[0].text
'''
tokenFile = open('resumptionTokens.csv','a')
tokenFile.write(curToken)
tokenFile.write('\n')
tokenFile.close()
print curToken


hasResToken = True
while (resToken):
    urlPage = 'http://export.arxiv.org/oai2/request?verb=ListRecords&resumptionToken=%s' % (curToken)
print data'''

