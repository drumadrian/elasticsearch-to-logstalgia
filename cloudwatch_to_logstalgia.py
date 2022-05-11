import boto3
from datetime import datetime, timedelta
import time

client = boto3.client('logs')

notes={
    # https://www.youtube.com/watch?v=Gj5SD2zMivU
    # https://docs.aws.amazon.com/cli/latest/reference/logs/start-query.html
    # https://docs.aws.amazon.com/AmazonCloudWatch/latest/logs/CWL_QuerySyntax.html
    # https://docs.aws.amazon.com/AmazonCloudWatch/latest/logs/CWL_QuerySyntax.html
    # https://stackoverflow.com/questions/53725133/amazon-cloudwatch-logs-insights-with-json-fields
    # https://www.geeksforgeeks.org/switch-case-in-python-replacement/
    # https://stackoverflow.com/questions/21070369/importerror-no-module-named-virtualenv
}




########################################################################################################################################################################
# Function to covert CloudTrail time to Apache Log file time for Logstalgia 
# Input Date and Time from CloudTrail:   2022-04-15T06:38:45Z
# Output Date and Time for Logstalgia:      22/Apr/2009:18:52:51 -0000
# https://docs.aws.amazon.com/awscloudtrail/latest/userguide/cloudtrail-log-file-examples.html
########################################################################################################################################################################
def convert_cloudtrail_time_to_apache_time_format(date_time_from_record):
    # Have:
    # 2022-04-15T06:38:45Z
    # Need:
    # 28/Apr/2020:22:44:49
    # Got:
    # 2020-Nov-23T07:43:07Z
    # 2021-Jan-1T06:23:08Z
    debug = False

    date_time_from_record = date_time_from_record.replace('Z', '')

    if debug:
        print('date_time_from_record=' + date_time_from_record)

    # time_from_record_list = time_from_record_after_replacement.split(':')
    split_date_time_from_record_list = date_time_from_record.split('-')
    year = split_date_time_from_record_list[0]
    month_number = split_date_time_from_record_list[1]

    # convert month number to month name
    datetime_object = datetime.strptime(month_number, "%m")
    month = datetime_object.strftime("%b")

    day_and_date_from_record = split_date_time_from_record_list[2]
    split_day_and_date_from_record_list = day_and_date_from_record.split("T")
    day = split_day_and_date_from_record_list[0]

    time_from_record = split_day_and_date_from_record_list[1]
    time_from_record_list = time_from_record.split(":")

    hour = time_from_record_list[0]
    minutes = time_from_record_list[1]
    seconds = time_from_record_list[2]

    if debug:
        print(day)
        print(month)
        print(year)
        print(hour)
        print(minutes)
        print(seconds)

    newtime = str( day + '/' + month + '/' + year + ':' + hour + ':' + minutes + ':' + seconds )

    if debug:
        print('newtime=' + newtime)

    return newtime


# query = "fields @timestamp, @message | parse @message \"username: * ClinicID: * nodename: *\" as username, ClinicID, nodename | filter ClinicID = 7667 and username='simran+test@abc.com'"  
query = "fields sourceIPAddress, userIdentity.invokedBy, eventCategory, eventTime, eventSource, eventName, awsRegion, additionalEventData.bytesTransferredOut, recipientAccountId, userAgent | limit 500"  

log_group = 'aws-cloudtrail-logs-696965430582-39a3ce5d'

start_query_response = client.start_query(
    logGroupName=log_group,
    startTime=int((datetime.today() - timedelta(hours=0.5)).timestamp()),
    endTime=int(datetime.now().timestamp()),
    queryString=query,
)

query_id = start_query_response['queryId']

response = None

while response == None or response['status'] == 'Running':
    # print('Waiting for query to complete ...')
    time.sleep(1)
    response = client.get_query_results(
        queryId=query_id
    )
# print(response)

########################################################################################################################################################################
# Parse and Print data from CloudWatch Insights 
    # Assign CloudTrail record fields to Apache log file variables
########################################################################################################################################################################
HOST            = "-"
USER            = "-"
USERNAME        = "-"
DATETIME        = "-"
TIMEZONE        = "-"
REQUESTVERB     = "-"
REQUESTURI      = "-"
RESPONSESTATUS  = "-"
SIZEOFREQUEST   = "-"
REFERER         = "-"
USERAGENT       = "-"

record_list = response['results']

for records in record_list:
    # print(records)
    for record in records:
        match record['field']:
            case 'sourceIPAddress':
                HOST = record['value']
            case 'userIdentity.invokedBy':
            #     USER = record['value']
                pass
            case 'eventCategory':
                USERNAME = record['value']
            case 'eventTime':
                CLOUDTRAILDATETIME = record['value']
                TIMEZONE = "-0000"
                DATETIME = convert_cloudtrail_time_to_apache_time_format(CLOUDTRAILDATETIME)
            case 'eventSource':
                REQUESTVERB = record['value']
            case 'eventName':
                REQUESTURI = record['value']
            case 'awsRegion':
                RESPONSESTATUS = record['value']
            case 'additionalEventData.bytesTransferredOut':
                SIZEOFREQUEST = record['value']
            case 'recipientAccountId':
                REFERER = record['value']
            case 'userAgent':
                # USERAGENT = record['value']
                pass
            case _:
                pass



        logstalgia_log_line = '%s %s %s [%s %s] \"%s %s HTTP/1.0\" %s %s \"%s\" \"%s\"' % (HOST,USER,USERNAME,DATETIME,TIMEZONE,REQUESTVERB,REQUESTURI,RESPONSESTATUS,SIZEOFREQUEST,REFERER,USERAGENT)
    
        print (logstalgia_log_line)

