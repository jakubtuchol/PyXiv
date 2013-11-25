import urllib
import xml.etree.ElementTree as ET

url = 'http://export.arxiv.org/oai2/request?verb=ListRecords&metadataPrefix=arXiv&from=2013-11-03&until=2013-11-19'
initialData = urllib.urlopen(url).read()
saveFile = open('metadata.xml','a')
saveFile.write(initialData)
saveFile.write('\n') #don't know structure of XML files-need to see if have newlines
saveFile.close()
xmlTree = ET.fromstring(initialData)
curToken = xmlTree.find("OAI-PMH/ListRecords/resumptionToken")
tokenType = type(curToken)
print tokenType
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

