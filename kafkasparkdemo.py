#import findsparkf
#indspark.init()
from json import loads
from pprint import pprint
import re
from pyspark.streaming.kafka import KafkaUtils
from pyspark.streaming import StreamingContext
from pyspark import SparkContext
from textblob import TextBlob
def analyze_sentiment(tweet):
        analysis = TextBlob(tweet)
        score = analysis.sentiment.polarity
        if analysis.sentiment.polarity > 0:
            return 'Positive',tweet,score
        elif analysis.sentiment.polarity == 0:
            return 'Neutre',tweet,score
        else:
            return 'Negative',tweet,score

if __name__=="__main__":
    sc = SparkContext(appName = "kafka spark demo")
    ssc = StreamingContext(sc,15)

    #liaison kafka spark
    message=KafkaUtils.createDirectStream(ssc,topics=['first_topic'],kafkaParams={"metadata.broker.list":"localhost:9092"})
   
    
    res = message.map(lambda x: tuple(x))
    res1 = res.map(lambda x : x[1].encode('utf-8'))
    res2= res1.map(lambda x : x.lower())
    res3=res2.map(lambda x : re.sub(r'@[A-Za-z0-9_]+'," ", x))
    res4=res3.map(lambda x : re.sub(r'#[A-Za-z0-9_]+'," ", x))
    res5=res4.map(lambda x : re.sub(r'^rt',"", x))

    res6=res5.map(lambda x : re.sub(r'http\S+', " ", x))
    res7=res6.map(lambda x : re.sub(r'www.\S+', " ", x))
    res8=res7.map(lambda x : re.sub(r'\[.*?\]'," ", x))
    res9=res8.map(lambda x : re.sub(r'[()!?]', " ", x))
    res10=res9.map(lambda x : re.sub(r'[^a-z0-9]'," ", x))
    res11=res10.map(lambda x : re.sub(r'^:'," ", x))

    
    
rdd = res11.map(lambda x :  analyze_sentiment(x))
ssc.checkpoint("/home/user/Bureau/Spark_project/result")

#rdd1=rdd.map(lambda x: (x[0],1)).reduceByKey(lambda a,b: a+b)
rdd2 = rdd.map(lambda x: (x[0],1)).reduceByKeyAndWindow( lambda a,b: a+b , 120, 60 )
rdd2.saveAsTextFiles("/home/user/Bureau/Spark_project/result")
rdd2.pprint()
ssc.start()
ssc.awaitTermination()