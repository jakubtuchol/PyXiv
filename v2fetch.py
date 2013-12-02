import xml.dom.pulldom
import codecs
import re
import time
import urllib2
import zlib

nDataBytes, nRawbytes, nRecoveries, maxRecoveries = 0, 0, 0, 3

def getFile(fetchBase, command, verbose=1, sleepTime=0):
    global nRecoveries, nDataBytes, nRawBytes
    if sleepTime:
        time.sleep(sleepTime)
    remoteAddr = fetchBase + command
    
    if verbose:
        print "\r", "getFile ... '%s'" % remoteAddr[-90:]
        
    try:
        remoteData = urllib2.urlopen(remoteAddr).read()
    except urllib2.HTTPError, exValue:
        if exValue.code == 503:
            retryWait = int(exValue.hdrs.get("Retry-After", "-1"))
            if retryWait < 0:
                return None
            print 'Waiting %d seconds' % retryWait
            return getFile(serverString, command, 0, retryWait)
        print exValue
        if nRecoveries < maxRecoveries:
            nRecoveries += 1
            return getFile(fetchBase, command, 1, 60)
        return
    nRawBytes += len(remoteData)

    # check for data compression
    try: 
        remoteData = zlib.decompressobj().decompress(remoteData)
    except:
        pass
    nDataBytes += len(remoteData)
    
    oaiError = re.search('<error *code=\"([^"]*)">(.*)</error>', remoteData)
    if oaiError:
        print "OAIERROR: code=%s '%s'" % (oaiError.group(1), oaiError.group(2))
    else:
        return remoteData

fetchBase = 'http://export.arxiv.org/oai2/request?verb=ListRecords'
fetchCommand = '&metadataPrefix=Arxiv'
outputFileName = 'metadata.xml'
print "Writing records to %s from archive %s" % (outputFileName, fetchBase)

outputFile = codecs.lookup('utf-8')[-1](file(outputFileName, 'wb'))
outputFile.write('<repository xmlns:oai_dc="http://www.openarchives.org/OAI/2.0/oai_dc/" \
 xmlns:dc="http://purl.org/dc/elements/1.1/" \
 xmlns:xsi="http://www.w3.org/2001/XMLSchema-instanc">\n')

data = getFile(fetchBase, fetchCommand)

recordCount = 0

while data:
    events = xml.dom.pulldom.parseString(data)
    for (event, node) in events:
        if event == "START_ELEMENT" and node.tagName == 'record':
            events.expandNode(node)
            node.writexml(outputFile)
            recordCount += 1
        resToken = re.search('<resumptionToken[^>]*>(.*)</resumptionToken>', data)
        if not resToken:
            break
        data = getFile(fetchBase, "&resumptionToken=%s" % mo.group(1)

outputFile.write('\n</repository>\n'), outputFile.close()
