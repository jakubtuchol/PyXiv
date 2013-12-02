import xml.dom.pulldom
import codecs
import re

serverBase = 'http://export.arxiv.org/oai2/'
requestBase = 'request?verb=ListRecords'
fetchCommand = '&metadataPrefix=Arxiv'
outputFileName = 'metadata.xml'
intialRequestString = outputFileName + requestBase
print "Writing records to %s form archive %s" % (outputFileName, initialRequestString + fetchCommand)

outputFile = codecs.lookup('utf-8')[-1](file(outputFileName, 'wb'))
outputFile.write('<repository xmlns:oai_dc="http://www.openarchives.org/OAI/2.0/oai_dc/" \
 xmlns:dc="http://purl.org/dc/elements/1.1/" \
 xmlns:xsi="http://www.w3.org/2001/XMLSchema-instanc">\n')

data = getFile(initialRequestString, fetchCommand)

recordCount = 0

while data:
    events = xml.dom.pulldom.parseString(data)
    for (event, node) in events:
        if event == "START_ELEMENT" and node.tagName == 'record':
            events.expandNode(node)
            node.writexml(outputFile)
            recordCount += 1
        resToken = re.search('<resumptionToken[^>]*>(.*)</resumptionToken>', data)
        if not resToken
