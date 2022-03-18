from cgitb import text
import tweepy
from tweepy import StreamRule
from pykafka import KafkaClient

def get_kafka_client():
    return KafkaClient(hosts='127.0.0.1:9092')
    
class IDPrinter(tweepy.StreamingClient):

    def on_tweet(self, tweet):
        #dataJson = tweet.decode('utf-8').replace("'","")
        
        #print(tweet)

        with open('dataTweets.txt', 'a', encoding="utf-8") as fd:
                fd.write(tweet.text)
        client = get_kafka_client()
        topic = client.topics['first_topic']
        producer = topic.get_sync_producer()
        producer.produce(tweet.text.encode('utf-8'))
        
    def on_errors(self, status):

        print(status)   


stream = IDPrinter("AAAAAAAAAAAAAAAAAAAAAFNQaAEAAAAABI%2F5h3g%2BR2wHvFWwBBpcoNe7aPw%3DICsBAQkbtUlT9IQU0at1lqWrZgcxNkKZwKctm6agCWERI6dl9o")


filterTweet= tweepy.StreamRule(value="covid19  lang:en")
stream.add_rules(filterTweet)
stream.filter()
'''
stream.add_rules(tweepy.StreamRule(["lang:fr"]), dry_run=True)
stream.filter(tweet_fields=["author_id","context_annotations","conversation_id","created_at","entities","geo","id","in_reply_to_user_id",
                                "lang","non_public_metrics","public_metrics","organic_metrics","promoted_metrics","possibly_sensitive","referenced_tweets",
                                "reply_settings","source","text","withheld"])'''