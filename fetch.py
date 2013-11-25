import urllib
#from lxml import etree

url = 'http://export.arxiv.org/oai2/request?verb=ListRecords&metadataPrefix=arXiv&from=2013-11-03&until=2013-11-19'
initialData = urllib.urlopen(url).read()

'''
xmlTree = etree.parse(initialData)
curToken = etree.find("resumptionToken")
'''
saveFile = open('metadata.xml','a')
saveFile.write(initialData)
saveFile.write('\n') #don't know structure of XML files-need to see if have newlines
saveFile.close()
'''
hasResToken = True
while (resToken):
    urlPage = 'http://export.arxiv.org/oai2/request?verb=ListRecords&resumptionToken=%s' % (curToken)
print data'''

