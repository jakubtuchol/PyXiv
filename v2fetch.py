# all data going to /glusterfs/users/metaknowledge/rawdata
import xml.dom.pulldom
import codecs
import re
import time
import urllib2
import zlib

nDataBytes, nRawBytes, nRecoveries, maxRecoveries = 0, 0, 0, 3

def getFile(fetchBase, command, verbose=1, sleepTime=0):
    global nRecoveries, nDataBytes, nRawBytes
    # sleep if server commands function to timeout
    if sleepTime:
        time.sleep(sleepTime)
    remoteAddr = fetchBase + command
    # verbose option used primarily when getting HTTP error from server
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
            return getFile(fetchBase, command, 0, retryWait)
        print exValue
        if nRecoveries < maxRecoveries:
            nRecoveries += 1
            return getFile(fetchBase, command, 1, 60)
        return
    nRawBytes += len(remoteData)
    # check for data compression and decompress if is present
    try: 
        remoteData = zlib.decompressobj().decompress(remoteData)
    except:
        pass
    nDataBytes += len(remoteData)
    # check and catch any errors
    oaiError = re.search('<error *code=\"([^"]*)">(.*)</error>', remoteData)
    if oaiError:
        print "OAIERROR: code=%s '%s'" % (oaiError.group(1), oaiError.group(2))
    else:
        return remoteData

if __name__ == "__main__":
    # url base for what our data
    fetchBase = 'http://export.arxiv.org/oai2/request?verb=ListRecords'
    # our URL option-- will be replaced by resumption token in next iteration
    fetchCommand = '&metadataPrefix=arXiv'
    # name for our output file
    outputFileName = 'metadata.xml'
    print "Writing records to %s from archive %s" % (outputFileName, fetchBase)

    # creating file for data output
    outputFile = codecs.lookup('utf-8')[-1](file(outputFileName, 'wb'))
    # creating XML wrapper around our data
    outputFile.write('<repository xmlns:oai_dc="http://www.openarchives.org/OAI/2.0/oai_dc/" \ xmlns:dc="http://purl.org/dc/elements/1.1/" \ xmlns:xsi="http://www.w3.org/2001/XMLSchema-instanc">\n')

    # getting our initial data
    data = getFile(fetchBase, fetchCommand)
    # initializing count of records that we're getting
    recordCount = 0
    
    # will loop while we're still getting data from the server
    while data:
        events = xml.dom.pulldom.parseString(data)
        for (event, node) in events:
            if event == "START_ELEMENT" and node.tagName == 'record':
                events.expandNode(node)
                node.writexml(outputFile)
                recordCount += 1
                print "Now getting record number " + str(recordCount)
                # parse resumption token from output
                resToken = re.search('<resumptionToken[^>]*>(.*)</resumptionToken>', data)
                # if we don't have the resumption token, we assume that we're done
                if not resToken:
                    break
                # call OAI API using reumption token
                data = getFile(fetchBase, "&resumptionToken=%s" % resToken.group(1))

    # wrap and close our output file
    outputFile.write('\n</repository>\n'), outputFile.close()
    print "\nRead %d bytes (%.2f compression)" % (nDataBytes, float(nDataBytes) / nRawBytes)
    print "Wrote out %d records" % recordCount
