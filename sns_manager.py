#!/usr/bin/env python
import logging
import random
import uuid

import argparse

from datetime import datetime
from decimal import Decimal
from pathlib import Path, PosixPath
import boto3
from botocore.exceptions import ClientError


# Logger ------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s %(module)s %(lineno)d - %(message)s',
)
log = logging.getLogger()

# sns ------------------------------------------------------------------

def create_sns_topic(topic_name):
    sns = boto3.client('sns')
    sns.create_topic(Name=topic_name)
    return True

def list_sns_topics(next_token=None):
    sns = boto3.client('sns')
    params = {'NextToken': next_token} if next_token else {}
    topics = sns.list_topics(**params)
    return topics.get('Topics', []), topics.get('NextToken', None)
    
def list_sns_subscriptions(next_token=None):
    sns = boto3.client('sns')
    params = {'NextToken': next_token} if next_token else {}
    subscriptions = sns.list_subscriptions(**params)
    return subscriptions.get('Subscriptions', []), subscriptions.get('NextToken', None)

def subscribe_sns_topic(topic_arn, mobile_number):
    sns = boto3.client('sns')
    params = {
    'TopicArn': topic_arn,
    'Protocol': 'sms',
    'Endpoint': mobile_number,
    }
    res = sns.subscribe(**params)
    print(res)
    return True
    
def send_sns_message(topic_arn, message):
    sns = boto3.client('sns')
    params = {
    'TopicArn': topic_arn,
    'Message': message,
    }
    res = sns.publish(**params)
    print(res)
    return True

def unsubscribe_sns_topic(subscription_arn):
    sns = boto3.client('sns')
    params = {
        'SubscriptionArn': subscription_arn,
    }
    res = sns.unsubscribe(**params)
    print(res)
    return True

def delete_sns_topic(topic_arn):
    # This will delete the topic and all it's subscriptions.
    sns = boto3.client('sns')
    sns.delete_topic(TopicArn=topic_arn)
    return True

# inputs ------------------------------------------------------------------
if __name__ == '__main__':    
    parser = argparse.ArgumentParser(description='AWS SNS Operation')
    subparsers=parser.add_subparsers(title="Commands", dest="command")
    
    sp_create_sns_topic=subparsers.add_parser('create_sns_topic',  help='Create SNS topic')
    sp_create_sns_topic.add_argument('topic_name', type=str, help='Topic name')
    sp_create_sns_topic.set_defaults(func=create_sns_topic)


    sp_list_sns_topics=subparsers.add_parser('list_sns_topics',  help='Create SNS topic')
    sp_list_sns_topics.set_defaults(func=list_sns_topics)
    

    sp_subscribe_sns_topic=subparsers.add_parser('subscribe_sns_topic',  help='Subscribe to SNS topic')
    sp_subscribe_sns_topic.add_argument('topic_arn',  help='SNS topic')
    sp_subscribe_sns_topic.add_argument('mobile_number',  help='Mobile number')
    sp_subscribe_sns_topic.set_defaults(func=subscribe_sns_topic)

    sp_list_sns_subscriptions=subparsers.add_parser('list_sns_subscriptions',  help='Create SNS topic')
    sp_list_sns_subscriptions.set_defaults(func=list_sns_subscriptions)
    
    sp_send_sns_message=subparsers.add_parser('send_sns_message',  help='Publish a message')
    sp_send_sns_message.add_argument('topic_arn',  help='SNS topic to be published')
    sp_send_sns_message.add_argument('message',  help='Message to be published')
    sp_send_sns_message.set_defaults(func=send_sns_message)
    
    sp_unsubscribe_sns_topic=subparsers.add_parser('unsubscribe_sns_topic',  help='Publish a message')
    sp_unsubscribe_sns_topic.add_argument('subscription_arn',  help='SNS Subscription you want to unsubscribe')
    sp_unsubscribe_sns_topic.set_defaults(func=unsubscribe_sns_topic)
    
    sp_delete_sns_topic=subparsers.add_parser('delete_sns_topic',  help='Publish a message')
    sp_delete_sns_topic.add_argument('topic_arn',  help='SNS topic to be deleted')
    sp_delete_sns_topic.set_defaults(func=delete_sns_topic)
    
    
    args = parser.parse_args()

    if hasattr(args, 'func'):
        func = args.func
        if func == create_sns_topic:
            create_sns_topic(args.topic_name)
            
        elif func == list_sns_topics:
            out = list_sns_topics()
            print(out)
            
        elif func == subscribe_sns_topic:
            subscribe_sns_topic(args.topic_arn, args.mobile_number)
            
        elif func == list_sns_subscriptions:
            out=list_sns_subscriptions()
            print(out)
        
        elif func == send_sns_message:
            send_sns_message(args.topic_arn, args.message)
            
        elif func == unsubscribe_sns_topic:
            unsubscribe_sns_topic(args.subscription_arn)
            
        elif func == delete_sns_topic:
            delete_sns_topic(args.topic_arn)
        print('Done')
    else:
        parser.print_help()
        
        
