# all data going to /glusterfs/users/metaknowledge/rawdata
import codecs   # used for creating output file
import re       # used for parsing timeout errors and resumptionTokens
import time     # to be used for sleeping 
import urllib2  # used for fetching data
import zlib     # used for checking compression levels

nDataBytes, nRawBytes, nRecoveries, maxRecoveries = 0, 0, 0, 3

def getFile(fetchBase, command, verbose=1, sleepTime=0):
    global nRecoveries, nDataBytes, nRawBytes
    # sleep if server commands function to timeout
    if sleepTime:
        time.sleep(sleepTime)
    remoteAddr = fetchBase + command
    # verbose option used primarily when getting HTTP error from server
    if verbose:
        print "\r", "getFile '%s'" % remoteAddr[-90:]        
    try:
        remoteData = urllib2.urlopen(remoteAddr).read()
    # checking for and handling HTTP error
    except urllib2.HTTPError, exValue:
        if exValue.code == 503:
            # parse data to check how long wants us to wait
            retryWait = int(exValue.hdrs.get("Retry-After", "-1"))
            if retryWait < 0:
                return None
            print 'Waiting %d seconds' % retryWait
            return getFile(fetchBase, command, 0, retryWait)
        print exValue
        # if server keeps continually failing, we stop trying
        # otherwise, keep trying with new timeout and verbosity set
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
    # url base for arXiv data location
    fetchBase = 'http://export.arxiv.org/oai2/request?verb=ListRecords'
    # our URL option-- will be replaced by resumption token in next iteration
    fetchCommand = '&metadataPrefix=arXiv'
    # name for our output file
    outputFileName = 'metadata.xml'
    print "Writing records to %s from archive %s" % (outputFileName, fetchBase)

    # creating file for data output
    outputFile = codecs.lookup('utf-8')[-1](file(outputFileName, 'wb'))

    # getting our initial data
    data = getFile(fetchBase, fetchCommand)
    
    # will loop while we're still getting data from the server
    while data:
        outputFile.write(data)
        outputFile.write('\n')
        # parse resumption token from output
        resToken = re.search('<resumptionToken[^>]*>(.*)</resumptionToken>', data)
        # if we don't have the resumption token, we assume that we're done
        if not resToken:
            break
        # call OAI API using reumption token
        data = getFile(fetchBase, "&resumptionToken=%s" % resToken.group(1))

    # wrap and close our output file
    print "\nRead %d bytes (%.2f compression)" % (nDataBytes, float(nDataBytes) / nRawBytes)
