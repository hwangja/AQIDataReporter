import boto3
import urllib.request
import json
import datetime
import os

arn = os.environ.get('arn')
api_key = os.environ.get('api_key')
zipcode = os.environ.get('zipcode')

rest_url = 'http://www.airnowapi.org/aq/observation/zipCode/current/?format=application/json&zipCode={0}&date={1}&distance=25&API_KEY={2}'
output = "On {0} in {1}, AQI is {2}."

def handler(event, context):
    """
    This is the driving function in AWS Lambda.

    This function is expected to be triggered by a Scheduled Event as described in http://docs.aws.amazon.com/lambda/latest/dg/invoking-lambda-function.html#supported-event-source-scheduled-event. Data available to be accessed in the event object can be seen in http://docs.aws.amazon.com/lambda/latest/dg/eventsources.html#eventsources-scheduled-event. It is important to note that available methods in different types of events may be different depending on the trigger.

    The context provides useful logging information, if necessary, to the Lambda function. More information can be found http://docs.aws.amazon.com/lambda/latest/dg/python-context-object.html

    For the purposes of this Scheduled Event, the script is self contained, and puts the formatted text of PM2.5 air quality levels in a specified zip code onto an SNS topic.

    """
    with urllib.request.urlopen(rest_url.format(zipcode, str(datetime.date.today()), api_key)) as f:
        data = json.loads(f.read().decode())

        for elem in data:
            if str(elem['ParameterName']) == "PM2.5":
                send_message(context.aws_request_id, elem['DateObserved'], int(elem['HourObserved']), elem['ReportingArea'], int(elem['AQI']))

def send_message(requestId, date_observed, hour_observed, reporting_area, aqi_value):
    """
    Makes decision on whether it's an appropriate time and critical enough quality to send a notification. If so, the notification is published to SNS

    """
    if (is_within_reportable_hours(hour_observed) and is_air_quality_bad_enough(aqi_value)):
        publish_aqi_message_to_sns(requestId, date_observed, reporting_area, aqi_value)
    else:
        print('Reporting conditions(hour_observed:' + str(hour_observed) + ', aqi_value:'
        + str(aqi_value) + ') do not meet reporting requirements. No message is published to SNS')

def publish_aqi_message_to_sns(requestId, date_observed, reporting_area, aqi_value):
    """
    Publishes the formatted AQI message to SNS
    """
    client = boto3.client('sns')
    client.publish(
        TopicArn=arn,
        Message=output.format(date_observed, reporting_area, aqi_value)
        )
    print(requestId + ' published formatted AQI message to SNS')

def is_within_reportable_hours(hour_observed):
    """
    Reportable hours are define as between 0500 and 2100 PST (as the only user is in PST)
    """
    return 5 <= hour_observed <= 21

def is_air_quality_bad_enough(aqi_value):
    """
    Any value above 50 is considered as harmful.
    """
    return 50 < aqi_value