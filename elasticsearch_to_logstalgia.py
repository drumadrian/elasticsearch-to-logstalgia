import time
import datetime
from datetime import datetime, date, timezone
# from datetime import timedelta
# import pytz
# import numpy
# import random
# import gzip
import zipfile
# import sys
import argparse
# from faker import Faker
# from random import randrange
from tzlocal import get_localzone
local = get_localzone()
from elasticsearch import Elasticsearch, RequestsHttpConnection
import os
from cmreslogging.handlers import CMRESHandler
import logging
import sys
from requests_aws4auth import AWS4Auth
import iso8601

########################################################################################################################################################################
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
# CloudTrail representative reference 
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
# Function to covert string to datetime 
# Input Date and Time from Elasticsearch:   2020-05-17T03:02:27+00:00
# Output Date and Time for Logstalgia:      22/Apr/2009:18:52:51 +1200
#       Returns the formatted date and time string OR the formatted timezone offset string
#       For example:    
#                   22/Apr/2009:18:52:51
#                 or
#                   +1200
########################################################################################################################################################################
def convert(date_time, parameter): 
    # print("\n\n\n\n.....")
    # print("\n date_time format = {}".format(type(date_time)))      #  <class 'str'>
    # print("\n date_time = {}".format(date_time))      #  2020-05-17T03:02:27+00:00
    
    # EXAMPLE iso8601datetime = iso8601.parse_date("2007-01-25T12:00:00Z")
    iso8601datetime = iso8601.parse_date(date_time)
    # print("\n iso8601datetime = {}".format(iso8601datetime))      #  2007-01-25 12:00:00+00:00

    # Convert input String to datetime object    Ex: dt = datetime.strptime("21/11/06 16:30", "%d/%m/%y %H:%M")
    # dt = datetime.strptime(date_time, format)
    # datetime_str = datetime.strptime(date_time, source_time_format) 
    # print("\n datetime_str = {}".format(datetime_str))      #  2007-01-25 12:00:00+00:00
    if parameter == 'dt':
        converted_dt = iso8601datetime.strftime('%d/%b/%Y:%H:%M:%S')
        # print('\nconverted_dt={}\n\n'.format(converted_dt))
        return converted_dt 

    if parameter == 'tz':
        converted_tz = iso8601datetime.strftime('%z')
        # print('\nconverted_tz={}\n\n'.format(converted_tz))
        return converted_tz 

    
########################################################################################################################################################################
# Function to Query data from Elasticsearch 
#       Returns reference to the response from Elasticsearch
########################################################################################################################################################################
def get_access_logs(elastic_client):
    elastic_client.indices.refresh(index=ACCESS_LOG_INDEX)
    result = elastic_client.search(
        index=ACCESS_LOG_INDEX,
        body={
            "query": {
                "range": {
                    "timestamp": {
                        "gt": "now-500m"
                    }
                }
            },
            "sort": [
                {
                "timestamp": {
                    "order": "asc"
                }
                }
            ]        
        }
    )
    return result


########################################################################################################################################################################
# Function to Parse and Print data from Elasticsearch 
########################################################################################################################################################################
def parse_and_print(result):
    for hit in result['hits']['hits']:
        # print("%(timestamp)s: %(bucket)s %(s3_key)s " % hit["_source"])
        # print("bucket {} ".format(hit["_source"]['bucket']))
        # print ("\n\n")

        ########################################################################################################################################################################
        # Assign Elasticsearch response to Apache log file fields variables
        ########################################################################################################################################################################
        hits_data = hit['_source']
        ip = hits_data["remote_ip"]
        identity_of_the_client = hits_data["remote_ip"]
        userid_of_requestor = hits_data["requester"]
        dt = convert(hits_data["timestamp"], 'dt')                
        tz = convert(hits_data["timestamp"], 'tz')
        vrb = hits_data['operation']
        request_uri_without_verb = hits_data['request_uri'].split(' ')
        # uri = request_uri_without_verb[1]
        # uri = hits_data['bucket'] + request_uri_without_verb[1]
        uri = request_uri_without_verb[1]
        resp = hits_data['status_code']
        byt = hits_data['object_size']
        referer = hits_data['referrer']
        useragent = hits_data['user_agent']

        # print("bucket {} ".format(hit["_source"]['bucket']))
        print('%s - - [%s %s] "%s %s HTTP/1.0" %s %s "%s" "%s"\n' % (ip, dt,tz, vrb,uri, resp,byt,referer,useragent))
        
        # Using hits_data['request_uri'] as vrb to get Verb and path using 1 field from Elasticsearch
        # print('%s - - [%s %s] "%s HTTP/1.0" %s %s "%s" "%s"\n' % (ip, dt,tz, uri, resp,byt,referer,useragent))








########################################################################################################################################################################
# Retrieve Credentials 
########################################################################################################################################################################
AWS_ACCESS_KEY_ID=os.environ['AWS_ACCESS_KEY_ID']
AWS_SECRET_ACCESS_KEY=os.environ['AWS_SECRET_ACCESS_KEY']
AWS_SESSION_TOKEN=os.environ['AWS_SESSION_TOKEN']
AWS_REGION='us-west-2'
HOSTS=[{'host': 'search-serverl-elasti-v0qj0qv8tl87-bpwrjbfvwgtvn76dibzk2ncuz4.us-west-2.es.amazonaws.com', 'port': 443}]
# HOSTS=[{'host': 'search-serverl-elasti-v0qj0qv8tl87-bpwrjbfvwgtvn76dibzk2ncuz4.us-west-2.es.amazonaws.com', 'port': 443}]
ACCESS_LOG_INDEX="accesslogstoawscloud"
PYTHON_LOG_INDEX="elasticsearch-to-logstalgia"


########################################################################################################################################################################
# Configure ElasticCloud connection and Python logging to Elasticcloud
########################################################################################################################################################################
# awsauth = AWS4Auth(AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION, 'es', session_token=AWS_SESSION_TOKEN)
# HTTP_AUTH_PASSWORD=os.environ['HTTP_AUTH_PASSWORD']

# # Configure ElasticCloud
# elasticcloud_client = Elasticsearch(
#     cloud_id="meetup:dXMtd2VzdC0yLmF3cy5mb3VuZC5pbyQyOTFhMWQ0NmViM2M0YmNhYjU0YTkyNGYzYzYyMDUxYiQ4ZTBlYzdmZmIxNmI0NjQ5YWMwYWI4ZWI5NGM1OWVhNg==",
#     http_auth=("elastic", HTTP_AUTH_PASSWORD))

# handler = CMRESHandler( hosts=HOSTS,
#                         cloud_id = '',
#                         http_auth = '',
#                         auth_type=CMRESHandler.AuthType.ELASTICCLOUD_AUTH,
#                         # aws_region=AWS_REGION,
#                         use_ssl=True,
#                         verify_ssl=True,
#                         es_additional_fields={'App': 'elasticsearch-to-logstalgia', 'Environment': 'Dev'},
#                         es_index_name=PYTHON_LOG_INDEX)


########################################################################################################################################################################
# Configure AWS Elasticsearch Domain connection and Python logging to AWS Elasticsearch Domain
########################################################################################################################################################################
awsauth = AWS4Auth(AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION, 'es', session_token=AWS_SESSION_TOKEN)
awscloud_client = Elasticsearch(
    hosts=HOSTS,
    http_auth=awsauth,
    use_ssl=True,
    verify_certs=True,
    connection_class=RequestsHttpConnection)

handler = CMRESHandler( hosts=HOSTS,
                        auth_type=CMRESHandler.AuthType.AWS_SIGNED_AUTH,
                        aws_access_key=AWS_ACCESS_KEY_ID,
                        aws_secret_key=AWS_SECRET_ACCESS_KEY,
                        aws_session_token=AWS_SESSION_TOKEN,
                        aws_region=AWS_REGION,
                        use_ssl=True,
                        verify_ssl=True,
                        es_additional_fields={'App': 'elasticsearch-to-logstalgia', 'Environment': 'Dev'},
                        es_index_name=PYTHON_LOG_INDEX)


########################################################################################################################################################################
# Create and Configure logging handler
########################################################################################################################################################################
log = logging.getLogger("elasticsearch-to-logstalgia")
log.setLevel(logging.DEBUG)
# log.setLevel(logging.ERROR)
log.addHandler(handler)
# logging.basicConfig(stream=sys.stdout, level=logging.ERROR)


########################################################################################################################################################################
# Test Logging
########################################################################################################################################################################
# print("hello world")
# log.debug("hello stdout world")
# log.info("hello AWS world")



########################################################################################################################################################################
# Loop forever while Querying data and printing data
########################################################################################################################################################################
elastic_client = awscloud_client
elastic_client.indices.refresh(index=ACCESS_LOG_INDEX)
sleep_seconds = 5

while True:
    ########################################################################################################################################################################
    # Query data from Elasticsearch
    # For each set of records, parse the data and print to stdout for Logstalgia
    ########################################################################################################################################################################
    # elastic_client = elasticcloud_client
    result = get_access_logs(elastic_client)
    # hits = result['hits']['total']['value']
    # print("Got {} Hits".format(hits))
    # all_hits = result['hits']['hits']
    parse_and_print(result)
    time.sleep(sleep_seconds)




python elasticsearch-to-logstalgia.py | logstagia -

