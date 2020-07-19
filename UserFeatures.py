from pyspark.sql.functions import col, udf, struct, when, sum, greatest, lit
from pyspark.sql.types import DateType, TimestampType
import time
import datetime

class UserFeatures:
    def __init__(self, user_df, tweet_df, sparkSession, method_name):
        self.user_df = user_df
        self.tweet_df = tweet_df
        self.sparkSession = sparkSession
        self.method_name = method_name
        self.list_screen_names = []
        self.list_categories = []
        self.results_df = None 
        
    def startCalculation(self):
        spark = self.sparkSession
        sc = spark.sparkContext
        
        #cache dataframes
        tw = self.tweet_df
        tw.cache()
        u = self.user_df
        u.cache()
        
        # calculate duration of dataset    
        dates = tw.select('created_at').rdd.map(lambda r: convert_twitter_date(r[0])).collect()
        duration_of_dataset = self.get_duration_of_dataset(dates)
    
        # list of names
        self.list_screen_names = u.select('screen_name').rdd.map(lambda r: r[0]).distinct().collect()
    
        #list of categories
        list_categories=tw.select("category").rdd.map(lambda r: r[0]).distinct().collect()
        list_categories.sort()
        self.list_categories=list_categories
        
        # format dates and remove hour info
        format_dates = udf(convert_twitter_date_noHour, DateType())
        updated_tweet_df=tw.withColumn("formatted_date", format_dates(tw["created_at"]))
    
        #calculate tweets count of all users
        joined_df=u.join(updated_tweet_df, u.id == updated_tweet_df.userId,how='left')
        tweets_total=joined_df.groupBy("id").count().orderBy('count', ascending=False).withColumnRenamed("count", "tweets_total")
        
        #calculate tweets count of all users by topic
        tweets_by_topic=joined_df.groupBy("id").pivot("category").count().fillna(0, subset=list_categories)
        tweets_by_topic_nested = tweets_by_topic.select("id", struct(list_categories).alias("dict_tweet_by_topic"))
    
        #calculate days posted of all users by topic
        days_posted_by_topic=joined_df.groupBy("id","formatted_date").pivot("category").count().fillna(0, subset=list_categories)
        for cat in list_categories:
            days_posted_by_topic= days_posted_by_topic.withColumn(cat, when(days_posted_by_topic[cat]>0, 1).otherwise(0))
        days_posted_by_topic_summed=days_posted_by_topic.groupBy("id").agg(*[sum(c).alias(c) for c in list_categories])
        days_posted_by_topic_nested = days_posted_by_topic_summed.select("id", struct(list_categories).alias("dict_days_posted_by_topic"))
        
        #join tweets_total, tweets_by_topic_nested and, days_posted_by_topic_nested
        temp_u=tweets_total.join(tweets_by_topic_nested,"id").join(days_posted_by_topic_nested,"id")
        
        #calculate focus rate
        for cat in list_categories:
            temp_u=temp_u.withColumn(cat, col("dict_tweet_by_topic.{}".format(cat))/greatest(lit(1),col("tweets_total")))
        temp_u = temp_u.select("id","tweets_total","dict_tweet_by_topic","dict_days_posted_by_topic", struct(list_categories).alias("dict_focus_rate"))
        
        #calculate activeness1
        for cat in list_categories:
            temp_u=temp_u.withColumn(cat, col("dict_days_posted_by_topic.{}".format(cat))/duration_of_dataset)
        temp_u = temp_u.select("id","tweets_total","dict_tweet_by_topic","dict_days_posted_by_topic", "dict_focus_rate", struct(list_categories).alias("dict_activeness_1"))
    
        #calculate activeness2
        for cat in list_categories:
            temp_u=temp_u.withColumn(cat, col("dict_tweet_by_topic.{}".format(cat))/duration_of_dataset)
        temp_u = temp_u.select("id","tweets_total","dict_tweet_by_topic","dict_days_posted_by_topic", "dict_focus_rate", "dict_activeness_1", struct(list_categories).alias("dict_activeness_2"))
    
        #calculate activeness3
        for cat in list_categories:
            temp_u=temp_u.withColumn(cat, col("dict_tweet_by_topic.{}".format(cat))*col("dict_days_posted_by_topic.{}".format(cat))/duration_of_dataset)
        temp_u = temp_u.select("id","tweets_total","dict_tweet_by_topic","dict_days_posted_by_topic", "dict_focus_rate", "dict_activeness_1", "dict_activeness_2", struct(list_categories).alias("dict_activeness_3"))
    
        #set results
        self.results_df=temp_u.select("id",struct(temp_u.columns[1:]).alias("user_features{}".format(("_"+self.method_name) if len(self.method_name)>0 else "")))
        
        #clear cache
#        spark.catalog.clearCache()
        
    def get_duration_of_dataset(self, dates):
        max_date = max(dates)
        min_date = min(dates)
        days = (max_date - min_date).days
        print('max_date {} min_date {} days {}'.format(max_date, min_date, days))
        return days
    
    
    def getScreenNames(self):
        return self.list_screen_names
    
    
    def getCategories(self):
        return self.list_categories
    
    
    def getResults(self):
        return self.results_df
    
    
    def getUserFeaturesTopics(self):
        return ['dict_activeness_1','dict_activeness_2','dict_activeness_3','dict_focus_rate']


def convert_twitter_date(created_at):
    ts = time.strftime('%Y-%m-%d %H:%M:%S', time.strptime(created_at, '%a %b %d %H:%M:%S +0000 %Y'))
    date_time_obj = datetime.datetime.strptime(ts, '%Y-%m-%d %H:%M:%S')
    return date_time_obj


def convert_twitter_date_noHour(created_at):
    ts = time.strftime('%Y-%m-%d', time.strptime(created_at, '%a %b %d %H:%M:%S +0000 %Y'))
    date_time_obj = datetime.datetime.strptime(ts, '%Y-%m-%d')
    return date_time_obj