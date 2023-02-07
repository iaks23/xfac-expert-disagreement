#Getting necessary imports
import requests
import os
import json
import pandas as pd
import csv
import datetime
import dateutil.parser
import unicodedata

#adding wait time between requests
import time

#Setting environment variable with Bearer Token

os.environ['TOKEN'] = 'AAAAAAAAAAAAAAAAAAAAAP6QhQEAAAAAcmwwpH6stIF9xv4Ro7j4sflUi9A%3Deul70hYGi59n7P5kLhqTLS6QGn1eNZra5EakPc8TD6k9HxBeSA'

def auth():
    return os.getenv('TOKEN')

def create_headers(bearer_token):
    headers = {"Authorization": "Bearer {}".format(bearer_token)}
    return headers

#Creating an API request

def create_url(keyword, start_date, end_date, max_results=500):

    search_url = "https://api.twitter.com/2/tweets/search/all"

    query_params = {'query': keyword,
                    'start_time': start_date,
                    'end_time': end_date,
                    'max_results': max_results,
                    'expansions': 'author_id,in_reply_to_user_id,geo.place_id',
                    'tweet.fields': 'id,text,author_id,in_reply_to_user_id,geo,conversation_id,created_at,lang,entities,public_metrics,referenced_tweets,reply_settings',
                    'place.fields': 'full_name,id,country,country_code,geo,name,place_type',
                    'media.fields': 'url',
                    'next_token': {}}
    return (search_url, query_params)


#Connecting to endpoint
def connect_to_endpoint(url, headers, params, next_token = None):
    params['next_token'] = next_token
    response = requests.request("GET", url, headers = headers, params = params)
    print("Endpoint Response Code: "+ str(response.status_code))
    if response.status_code != 200:
        raise Exception(response.status_code, response.text)
    return response.json()


#Inputs for request
bearer_token = auth()
headers = create_headers(bearer_token)
keyword = "(expert OR research OR scientist OR science OR academic OR lecturer OR professor) covid (uncertain OR statistical OR likely OR unlikely OR ambiguous OR consensus OR unreliable OR disagree OR conflict) has:media_link lang:en"
#start_time = "2022-08-03T00:00:00.000Z"
#end_time = "2022-09-01T00:00:00.000Z"
max_results = 500 #default parameter
start_list = ['2021-08-01T00:00:00.000Z',
              
            ]

end_list = ['2021-08-31T00:00:00.000Z',
            
            ]

total_tweets = 0

csvFile = open("/Users/akshayaparthasarathy/Desktop/UNI/research/covid_dataset/august21/august21.csv","a",newline ="", encoding = 'utf-8')

csvWriter = csv.writer(csvFile)

csvWriter.writerow(['author id', 'created_at', 'tweet_id','tweet','entities','like_count','quote_count','retweet_count'])
csvFile.close()

def append_to_csv(json_response, fileName):
    
    counter = 0

    csvFile = open(fileName,"a",newline="",encoding='utf-8')
    
    csvWriter = csv.writer(csvFile)
    csvWriter.writerow(['author id', 'created_at', 'tweet_id','tweet','entities','like_count','quote_count','retweet_count'])


    for tweet in json_response['data']:
        author_id = tweet['author_id']

        created_at = dateutil.parser.parse(tweet['created_at'])

        if ('entities' in tweet):
            entities = tweet['entities']
            
        else:

            entities = ' '


        

        tweet_id = tweet['id']

        #source = tweet['source']

        #Tweet metrics
        like_count = tweet['public_metrics']['like_count']
        quote_count = tweet['public_metrics']['quote_count']
        retweet_count = tweet['public_metrics']['retweet_count']

        

        text = tweet['text']

        res = [author_id, created_at, tweet_id, text, entities, like_count, quote_count, retweet_count]

        csvWriter.writerow(res)
        counter += 1

    csvFile.close()

    print("# of tweets added from this response:", counter)

for i in range(0, len(start_list)):
    count = 0
    max_count = 8000
    flag = True
    next_token = None 

    while flag:
        if count >= max_count:
            break
        print("---------------")
        print("Token: ", next_token)
        url = create_url(keyword, start_list[i], end_list[i], max_results)
        json_response = connect_to_endpoint(url[0], headers, url[1], next_token)
        result_count = json_response['meta']['result_count']

        if 'next_token' in json_response['meta']:
            next_token = json_response['meta']['next_token']
            print('Next Token: ', next_token)

            if result_count is not None and result_count > 0 and next_token is not None:
                print("Start Date: ", start_list[i])
                append_to_csv(json_response, "/Users/akshayaparthasarathy/Desktop/UNI/research/covid_dataset/august21/august21.csv")
                count += result_count
                total_tweets += result_count 
                print("Total # of Tweets added: ", total_tweets)
                time.sleep(5)
            else:
                if result_count is not None and result_count > 0:
                    print("Start Date: ", start_list[i])
                    append_to_csv(json_response, "/Users/akshayaparthasarathy/Desktop/UNI/research/covid_dataset/august21/august21.csv")
                    count += result_count
                    total_tweets += result_count
                    print("Total # of tweets added: ", total_tweets)
                    time.sleep(5)
                flag = False
                next_token = None 
            time.sleep(5)
    
print("Total number of results: ", total_tweets)


#print(json.dumps(json_response, indent=4, sort_keys=True))

#Saving responses to custom CSV

#csvFile = open("/Users/akshayaparthasarathy/Desktop/UNI/research/final_outputs/output.csv","a",newline="",encoding='utf-8')
#csvWriter = csv.writer(csvFile)

#csvWriter.writerow(['author id', 'created_at', 'tweet_id','source','tweet','entities','like_count','quote_count','retweet_count'])

#url = create_url(keyword, start_time, end_time, max_results)
#json_response = connect_to_endpoint(url[0], headers, url[1])




    


#append_to_csv(json_response, "/Users/akshayaparthasarathy/Desktop/UNI/research/final_outputs/outputs.csv")




    