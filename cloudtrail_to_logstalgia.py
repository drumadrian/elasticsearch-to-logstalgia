import time
import datetime
# from datetime import datetime, date, timezone
# # from datetime import timedelta
# # import pytz
# # import numpy
# # import random
# # import gzip
# import zipfile
# # import sys
# import argparse
# # from faker import Faker
# # from random import randrange
# from tzlocal import get_localzone
# local = get_localzone()
# from elasticsearch import Elasticsearch, RequestsHttpConnection
# import os
# from cmreslogging.handlers import CMRESHandler
# import logging
# import sys
# from requests_aws4auth import AWS4Auth
# import iso8601

notes={    ########################################################################################################################################################################
    # Notes: 
    # https://github.com/acaudwell/Logstalgia
    # https://github.com/kiritbasu/Fake-Apache-Log-Generator
    # https://bitbucket.org/micktwomey/pyiso8601/src/default/
    # https://pypi.org/project/iso8601/
    # https://docs.python.org/3/library/datetime.html#strftime-strptime-behavior
    # https://elasticsearch-py.readthedocs.io/en/master/
    # Apache log reference:
    # 160.178.3.18 - - [28/Apr/2020:22:47:42 -0700] "PUT /explore HTTP/1.0" 200 4880 "https://www.edwards-brown.com/login.jsp" "Mozilla/5.0 (Windows NT 5.1; am-ET; rv:1.9.2.20) Gecko/2015-07-01 23:17:10 Firefox/5.0"
    # 
    # Example CloudTrail representative reference 
    # user1 - - [28/Apr/2020:22:44:49 -0700] "GET bucket://object HTTP/1.0" 200 5003 "https://banks.com/list/author.html" "Mozilla/5.0 (Windows NT 5.1) AppleWebKit/533.1 (KHTML, like Gecko) Chrome/58.0.822.0 Safari/533.1"
    ########################################################################################################################################################################

    ########################################################################################################################################################################
    # Example Access Log entry in Elasticsearch
    ########################################################################################################################################################################
            # "_source" : {
            #   "bucket_owner" : "b'2279185f7619a617e0a834c7f0660e4b09ea7f842f9d768d39109ee6e4cdf522",
            #   "bucket" : "amazon-s3-bucket-load-test-storagebucket-iul1vaxwkpbd",
            #   "timestamp" : "2020-05-17T03:00:15+00:00",
            #   "remote_ip" : "18.236.196.92",
            #   "requester" : "arn:aws:sts::696965430582:assumed-role/Amazon-S3-Bucket-Load-Test-EcsTaskExecutionRole-AD22BDSDSN84/415044f6-7f32-4966-af96-1bca2385a049",
            #   "request_id" : "5396B0483457C02D",
            #   "operation" : "REST.GET.OBJECT",
            #   "s3_key" : "483790_03%253A00%253A10.483790_diagram.png",
            #   "request_uri" : "GET /483790_03%3A00%3A10.483790_diagram.png HTTP/1.1",
            #   "status_code" : 200,
            #   "error_code" : null,
            #   "bytes_sent" : 98831,
            #   "object_size" : 98831,
            #   "total_time" : 12,
            #   "turn_around_time" : 11,
            #   "referrer" : null,
            #   "user_agent" : "Boto3/1.12.10 Python/3.7.6 Linux/4.14.158-129.185.amzn2.x86_64 exec-env/AWS_ECS_FARGATE Botocore/1.15.10",
            #   "version_id" : null
            # }

    ########################################################################################################################################################################
    # Example Apache log format OUTPUT fields to Logstalgia
    ########################################################################################################################################################################
            # ip = principle_identity_arn
            # identity_of_the_client = "-"
            # userid_of_requestor = "-"
            # dt = nowtime.strftime('%d/%b/%Y:%H:%M:%S')
            # tz = datetime.datetime.now(local).strftime('%z')
            # vrb = numpy.random.choice(verb,p=[0.6,0.1,0.1,0.2])
            # uri = random.choice(resources)
            # resp = numpy.random.choice(response,p=[0.9,0.04,0.02,0.04])
            # byt = int(random.gauss(5000,50))
            # referer = faker.uri()
            # useragent = "unset-useragent"


    ########################################################################################################################################################################
    # Create and Configure logging handler
    ########################################################################################################################################################################
    # log = logging.getLogger("cloudtrail-to-logstalgia")
    # log.setLevel(logging.DEBUG)
    # log.setLevel(logging.ERROR)
    # logging.basicConfig(stream=sys.stdout, level=logging.ERROR)


    ########################################################################################################################################################################
    # Test Logging
    ########################################################################################################################################################################
    # print("hello world")
    # log.debug("hello stdout world")
    # log.info("hello AWS world")

    # https://aws.amazon.com/blogs/big-data/snakes-in-the-stream-feeding-and-eating-amazon-kinesis-streams-with-python/

    
}
notes2 = {
    # dyn-530.optus.com.au 
    # - 
    # - 
    # [22/Apr/2009:18:52:51 +1200] 
    # "GET /images/photos/5332.jpg HTTP/1.1" 
    # 200 
    # 244 
    # "-" 
    # "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; .NET CLR 1.1.4322)" 
    # "-"
    }


example_cloud_trail_record={
  "Records": [
    {
      "eventVersion": "1.08",
      "userIdentity": {
        "type": "AWSService",
        "invokedBy": "cloudtrail.amazonaws.com"
      },
      "eventTime": "2022-04-15T06:38:45Z",
      "eventSource": "s3.amazonaws.com",
      "eventName": "GetBucketAcl",
      "awsRegion": "us-west-2",
      "sourceIPAddress": "cloudtrail.amazonaws.com",
      "userAgent": "cloudtrail.amazonaws.com",
      "requestParameters": {
        "bucketName": "s3-to-elasticsearch-clou-cloudtraillogbucketaf9a1-1p3u2o9kh7caj",
        "Host": "s3-to-elasticsearch-clou-cloudtraillogbucketaf9a1-1p3u2o9kh7caj.s3.us-west-2.amazonaws.com",
        "acl": ""
      },
      "responseElements": "",
      "additionalEventData": {
        "SignatureVersion": "SigV4",
        "CipherSuite": "ECDHE-RSA-AES128-GCM-SHA256",
        "bytesTransferredIn": 0,
        "AuthenticationMethod": "AuthHeader",
        "x-amz-id-2": "R3KpOBWAHM8IeUi0rD+1/dOiP77zfvQ6EA9V+SXzevnVlgId7ZJzwnNaA5rW8IYp++n6vZe3PUk=",
        "bytesTransferredOut": 550
      },
      "requestID": "ZJ3E5BXGRXRM65WZ",
      "eventID": "0c25202e-64df-4fd7-b91a-1e0ce4317c90",
      "readOnly": True,
      "resources": [
        {
          "accountId": "696965430582",
          "type": "AWS::S3::Bucket",
          "ARN": "arn:aws:s3:::s3-to-elasticsearch-clou-cloudtraillogbucketaf9a1-1p3u2o9kh7caj"
        }
      ],
      "eventType": "AwsApiCall",
      "managementEvent": True,
      "recipientAccountId": "696965430582",
      "sharedEventID": "b46e49e9-3690-4693-b887-4dc596486f95",
      "eventCategory": "Management"
    }
  ]
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
    datetime_object = datetime.datetime.strptime(month_number, "%m")
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


query = "fields @timestamp, @message | sort @timestamp asc | parse @message \"username: * ClinicID: * nodename: *\" as username, ClinicID, nodename | filter ClinicID = 7667 and username='simran+test@abc.com'"  

fields @message
| sort @timestamp asc
| parse @message "awsRegion *" as awsRegion
| limit 5

parse 'awsRegion: *' as awsRegion
| limit 5


def get_logstalgia_formatted_cloud_trail_record(example_cloud_trail_record):
    # log.info("get_random_cloud_trail_record(): STARTED")
    cloud_trail_record = {}
    cloud_trail_record["HOST"]            = example_cloud_trail_record["sourceIPAddress"]
    cloud_trail_record["USER"]            = example_cloud_trail_record["userIdentity"]["invokedBy"]
    cloud_trail_record["USERNAME"]        = example_cloud_trail_record["eventCategory"]
    cloud_trail_record["DATETIME"]        = example_cloud_trail_record["eventTime"]
    # cloud_trail_record["TIMEZONE"]        = example_cloud_trail_record[""]
    cloud_trail_record["TIMEZONE"]        = "-0000"
    cloud_trail_record["REQUESTVERB"]     = example_cloud_trail_record["eventSource"]
    cloud_trail_record["REQUESTURI"]      = example_cloud_trail_record["eventName"]
    cloud_trail_record["RESPONSESTATUS"]  = example_cloud_trail_record["awsRegion"]
    cloud_trail_record["SIZEOFREQUEST"]   = example_cloud_trail_record["additionalEventData"]["bytesTransferredOut"]
    cloud_trail_record["REFERER"]         = example_cloud_trail_record["resources"][0]["accountId"]
    cloud_trail_record["USERAGENT"]       = example_cloud_trail_record["userAgent"]

    return cloud_trail_record


########################################################################################################################################################################
# Parse and Print data from Elasticsearch 
    # Assign Elasticsearch response to Apache log file variables
########################################################################################################################################################################
def parse_cloudtrail_record_to_logstalgia_log_line(logstalgia_cloudtrail_record):

    logstalgia_cloudtrail_record["APACHEDATETIME"] = convert_cloudtrail_time_to_apache_time_format(logstalgia_cloudtrail_record["DATETIME"])

    HOST            = logstalgia_cloudtrail_record["HOST"]
    USER            = logstalgia_cloudtrail_record["USER"]
    USERNAME        = logstalgia_cloudtrail_record["USERNAME"]
    DATETIME        = logstalgia_cloudtrail_record["APACHEDATETIME"]
    TIMEZONE        = logstalgia_cloudtrail_record["TIMEZONE"]
    REQUESTVERB     = logstalgia_cloudtrail_record["REQUESTVERB"]
    REQUESTURI      = logstalgia_cloudtrail_record["REQUESTURI"]
    RESPONSESTATUS  = logstalgia_cloudtrail_record["RESPONSESTATUS"]
    SIZEOFREQUEST   = logstalgia_cloudtrail_record["SIZEOFREQUEST"]
    REFERER         = logstalgia_cloudtrail_record["REFERER"]
    USERAGENT       = logstalgia_cloudtrail_record["USERAGENT"]


    # print("""user1 - - [28/Apr/2020:22:44:49 -0700] "GET bucket://object HTTP/1.0" 200 5003 "https://banks.com/list/author.html" "Mozilla/5.0 (Windows NT 5.1) AppleWebKit/533.1 (KHTML, like Gecko) Chrome/58.0.822.0 Safari/533.1\"""")
    # random_cloud_trail_record = "" + HOST + " " + USER + " " + USERNAME + " " + TIMESTAMP + " " + REQUEST + " " + RESPONSESTATUS + " " + SIZEOFREQUEST + " " + REFERER + " " + USERAGENT + " " + "-"
    # print('%s - - [%s %s] "%s %s HTTP/1.0" %s %s "%s" "%s"\n' % (ip, dt,tz, vrb,uri, resp,byt,referer,useragent))
    logstalgia_log_line = '%s %s %s [%s %s] \"%s %s HTTP/1.0\" %s %s \"%s\" \"%s\"' % (HOST,USER,USERNAME,DATETIME,TIMEZONE,REQUESTVERB,REQUESTURI,RESPONSESTATUS,SIZEOFREQUEST,REFERER,USERAGENT)
    # logstalgia_log_line = str("%s %s %s [%s %s] \"%s %s HTTP/1.0\" %s %s \"%s\" \"%s\"")
    return logstalgia_log_line



########################################################################################################################################################################
# Loop forever while Querying data and printing data
########################################################################################################################################################################
    ########################################################################################################################################################################
    # (originally) Query data from Elasticsearch
    # (now) get randomly examples for each field in a CloudTrail record
    # parse the CloudTrail record and print to stdout in apache log format for Logstalgia 
    ########################################################################################################################################################################

while True:
    logstalgia_cloudtrail_record = get_logstalgia_formatted_cloud_trail_record(example_cloud_trail_record["Records"][0])
    logstalgia_log_line = parse_cloudtrail_record_to_logstalgia_log_line(logstalgia_cloudtrail_record)
    print(logstalgia_log_line)
    # time.sleep(1)



########################################################################################################################################################################
# How to run this application from your shell CLI
########################################################################################################################################################################
# python cloudtrail_to_logstalgia.py | logstagia -

