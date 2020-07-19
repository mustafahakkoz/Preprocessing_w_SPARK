## Preprocessing with SPARK

Data manipulation and feature extraction with SPARK, network calcuations with Neo4j.

![Spark-Neo4j Pipeline](https://github.com/mustafahakkoz/Preprocessing_w_SPARK/blob/master/img/spark-neo4j.png)


# [Main.py](https://github.com/mustafahakkoz/Preprocessing_w_SPARK/blob/master/Main.py)  
Setting MongoDB connection and paths.

# [RunCalculators.py](https://github.com/mustafahakkoz/Preprocessing_w_SPARK/blob/master/RunCalculators.py)  
Setting Spark and Neo4j connections, importing and exporting data, handling preprocessing and normalization.

# [UserFeatures.py](https://github.com/mustafahakkoz/Preprocessing_w_SPARK/blob/master/UserFeatures.py)  
Calculating twitter-user features by manipulating and aggregating data by Spark methods.

# [CentralityMeasures.py](https://github.com/mustafahakkoz/Preprocessing_w_SPARK/blob/master/CentralityMeasures.py)  
Running ETL methods for Neo4J and calculating graph centrality measures by Cypher queries.

# PREREQUISITES FOR SPARK  
1. install pyspark --> pip install pyspark  
2. install mongo-spark-connector --> pyspark --packages org.mongodb.spark:mongo-spark-connector_2.11:2.4.1  
or download with dependencies https://jar-download.com/artifacts/org.mongodb.spark/mongo-spark-connector_2.11/2.4.1/source-code  

# PREREQUISITES FOR NEO4J  
1. install neo4j-desktop --> https://neo4j.com/download/  
2. create an empty neo4j graph and install the plug-ins: "graph algorithms" and "APOC". set auth key as "12345678"  
3. pip install neo4j-driver

# STRUCTURES OF DATABASES  
1. tweets database should be labeled by "category" column.  
2. tweets database should also contain "id", "user.id", "user.screen_name", "created_at" columns.  
3. user database should contain "id" and "screen_name" columns.  
4. edges file is a json file with columns "Source" and "Target", which defines a relationship between them.

# STRUCTURE OF OUTPUT JSON  
root  
 |-- id: string (nullable = true)  
 |-- user_features: struct (nullable = true)  
 |    |-- dict_activeness_1: struct (nullable = true)  
 |    |    |-- category1: double (nullable = true)  
 |    |    |-- category2: double (nullable = true)  
 |    |-- dict_activeness_2: struct (nullable = true)  
 |    |    |-- category1: double (nullable = true)  
 |    |    |-- category2: double (nullable = true)  
 |    |-- dict_activeness_3: struct (nullable = true)  
 |    |    |-- category1: double (nullable = true)  
 |    |    |-- category2: double (nullable = true)  
 |    |-- dict_days_posted_by_topic: struct (nullable = true)  
 |    |    |-- category1: long (nullable = true)  
 |    |    |-- category2: long (nullable = true)  
 |    |-- dict_focus_rate: struct (nullable = true)  
 |    |    |-- category1: double (nullable = true)  
 |    |    |-- category2: double (nullable = true)  
 |    |-- dict_tweet_by_topic: struct (nullable = true)  
 |    |    |-- category1: long (nullable = true)  
 |    |    |-- category2: long (nullable = true)  
 |    |-- tweets_total: long (nullable = true)  
 |-- centralities: struct (nullable = true)  
 |    |-- betweennessCentrality: struct (nullable = true)  
 |    |    |-- category1: double (nullable = true)  
 |    |    |-- category2: double (nullable = true)  
 |    |-- closenessCentrality: struct (nullable = true)  
 |    |    |-- category1: double (nullable = true)  
 |    |    |-- category2: double (nullable = true)  
 |    |-- degreeCentrality: struct (nullable = true)  
 |    |    |-- category1: double (nullable = true)  
 |    |    |-- category2: double (nullable = true)  
 |    |-- pageRank: struct (nullable = true)  
 |    |    |-- category1: double (nullable = true)  
 |    |    |-- category2: double (nullable = true)  


