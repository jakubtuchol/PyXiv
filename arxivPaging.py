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
from calendar import monthrange # to be used for getting the number of days in a given month
from datetime import datetime, date

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

def valid_date(datestring):
    try:
        datetime.strptime(datestring, '%Y-%m')
        return True
    except ValueError:
        return False

if __name__ == "__main__":
    # prompt the user to enter in a start date
    beginDateString = raw_input("Please enter in the month you would like to BEGIN downloading from, in 'YYYY-MM' format: ")
    # check if the user has input an properly formatted date
    while (not valid_date(beginDateString)):
        beginDateString = raw_input("Please make sure your input is in 'YYYY-MM' format, using only digits for the year and month: ")
    # once we have a properly formatted start date, create a date object for the start date
    startDate = datetime.strptime(beginDateString, '%Y-%m').date()
    
    # prompt the user to enter in an end date
    endDateString = raw_input("Please enter in the last month you would like to download, in 'YYYY-MM' format: ")
    # check if the user has input an properly formatted date
    while (not valid_date(endDateString)):
        endDateString = raw_input("Please make sure your input is in 'YYYY-MM' format, using only digits for the year and month: ")
    # reconstruct the end date to reflect the last day of the given month
    initialEndDate = datetime.strptime(endDateString, '%Y-%m').date()
    endDay = monthrange(initialEndDate.year, initialEndDate.month)[1]
    endDate = datetime.date(initialEndDate.year, initialEndDate.month, endDay)
    
    # url base for arXiv data location
    fetchBase = 'http://export.arxiv.org/oai2/request?verb=ListRecords'
    # our URL base option-- will be replaced by resumption token if necessary
    fetchPrefix = '&metadataPrefix=arXiv'
    
    # constructing the OAI command for specific start and end time for dates
    dateCommand = '&from=' + startDate.isoformat() + '&until=' + endDate.isoformat()
    print "We're fetching data from " + startDate.isoformat() + " until " + endDate.isoformat()
    
    # creating a file to write this data to, labeled by the start and end dates
    outputFileName = startDate.isoformat() + "_to_" + endDate.isoformat()
    print "Writing records to %s from archive %s" % (outputFileName, fetchBase)
    outputFile = codecs.lookup('utf-8')[-1](file(outputFileName, 'wb'))
    
    # fetching the data
    getDate(fetchBase, fetchPrefix, dateCommand, outputFile)
    print "\nRead %d bytes (%.2f compression)" % (nDataBytes, float(nDataBytes) / nRawBytes)
    print "Finished getting data"        
            
    
