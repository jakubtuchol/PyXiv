'''
This script will serve as a proof of concept for arxiv paging
We will be paging the arXiv servers for papers dating between
January 2011 and December 2012, and will save each of the 
returns in individual files.
'''
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

def getDate(fetchBase, commandPrefix, date, outputFile):
    # concatenate together the prefix and the date
    fetchCommand = commandPrefix + date
    # get the data
    dataChunk = getFile(fetchBase, fetchCommand)
    # will loop while we're still getting data from the server
    while dataChunk:
        outputFile.write(dataChunk)
        outputFile.write('\n')
        # parse resumption token from output
        resToken = re.search('<resumptionToken[^>]*>(.*)</resumptionToken>', dataChunk)
        # if we don't have the resumption token, we assume that we're done
        if not resToken:
            break
        # call OAI API using reumption token
        dataChunk = getFile(fetchBase, "&resumptionToken=%s" % resToken.group(1))
    outputFile.close()
    print "Finished writing " + date

if __name__ == "__main__":
    # url base for arXiv data location
    fetchBase = 'http://export.arxiv.org/oai2/request?verb=ListRecords'
    # our URL base option-- will be replaced by resumption token if necessary
    fetchPrefix = '&metadataPrefix=arXiv'
    months = range(1,13)
    years = [2011, 2012]
    # initializing a nested loop over the years and months
    # this loop will first construct our argument commands, and then 
    # call the getDate function, which will construct an output file
    # and proceed to query the database with the given arguments until 
    # it cannot find a resumptionToken
    for year in years:
        for month in months:
            # need to construct a proper double-digit month
            if month < 10:
                monthBound = '0' + str(month)
            else:
                monthBound = str(month)
            # setting value for upper bound of day for any given mont
            if (month % 2 == 1):
                endDay = 31
            # gott deal with February
            elif (month == 2):
                # 2012 is a leap year
                if (year == 2012):
                    endDay = 29
                else:
                    endDay = 28
            # constructing date specification for OAI command
            dateCommand = '&from=' + str(year) + '-' + monthBound + '-01&until=' + str(year) + '-' + monthBound + '-' + str(endDay)
            # printing date: for debugging purposes
            print "We're fetching these dates: " + dateCommand
            
            # creating our output file-- will have a separate output file for each mont
            outputFileName = str(year) + '-' + monthBound + '.xml'
            print "Writing records to %s from archive %s" % (outputFileName, fetchBase)
            outputFile = codecs.lookup('utf-8')[-1](file(outputFileName, 'wb'))
            
            getDate(fetchBase, fetchPrefix, dateCommand, outputFile)
            print "\nRead %d bytes (%.2f compression)" % (nDataBytes, float(nDataBytes) / nRawBytes)
    print "Finished getting data"
    
