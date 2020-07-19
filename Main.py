from RunCalculators import RunCalculators
import os

######################### PREREQUISITES FOR SPARK #############################
# 1. install pyspark --> pip install pyspark
# 2. install mongo-spark-connector --> pyspark --packages org.mongodb.spark:mongo-spark-connector_2.11:2.4.1
# or download with dependencies https://jar-download.com/artifacts/org.mongodb.spark/mongo-spark-connector_2.11/2.4.1/source-code

######################### PREREQUISITES FOR NEO4J #############################
# 1. install neo4j-desktop --> https://neo4j.com/download/
# 2. create an empty neo4j graph and install the plug-ins: "graph algorithms" and "APOC". set auth key as "12345678"
# 3. pip install neo4j-driver

######################### STRUCTURES OF DATABASES #############################
# 1. tweets database should be labeled by "category" column.
# 2. tweets database should also contain "id", "user.id", "user.screen_name", "created_at" columns.
# 3. user database should contain "id" and "screen_name" columns.
# 4. edges file is a json file with columns "Source" and "Target", which defines a relationship between them.

######################### STRUCTURE OF OUTPUT JSON ############################
#root
# |-- id: string (nullable = true)
# |-- user_features: struct (nullable = true)
# |    |-- dict_activeness_1: struct (nullable = true)
# |    |    |-- category1: double (nullable = true)
# |    |    |-- category2: double (nullable = true)
# |    |-- dict_activeness_2: struct (nullable = true)
# |    |    |-- category1: double (nullable = true)
# |    |    |-- category2: double (nullable = true)
# |    |-- dict_activeness_3: struct (nullable = true)
# |    |    |-- category1: double (nullable = true)
# |    |    |-- category2: double (nullable = true)
# |    |-- dict_days_posted_by_topic: struct (nullable = true)
# |    |    |-- category1: long (nullable = true)
# |    |    |-- category2: long (nullable = true)
# |    |-- dict_focus_rate: struct (nullable = true)
# |    |    |-- category1: double (nullable = true)
# |    |    |-- category2: double (nullable = true)
# |    |-- dict_tweet_by_topic: struct (nullable = true)
# |    |    |-- category1: long (nullable = true)
# |    |    |-- category2: long (nullable = true)
# |    |-- tweets_total: long (nullable = true)
# |-- centralities: struct (nullable = true)
# |    |-- betweennessCentrality: struct (nullable = true)
# |    |    |-- category1: double (nullable = true)
# |    |    |-- category2: double (nullable = true)
# |    |-- closenessCentrality: struct (nullable = true)
# |    |    |-- category1: double (nullable = true)
# |    |    |-- category2: double (nullable = true)
# |    |-- degreeCentrality: struct (nullable = true)
# |    |    |-- category1: double (nullable = true)
# |    |    |-- category2: double (nullable = true)
# |    |-- pageRank: struct (nullable = true)
# |    |    |-- category1: double (nullable = true)
# |    |    |-- category2: double (nullable = true)

if __name__ == '__main__':
    #Before running the program, set mongoDBs and write their names here.
    #Also, don't for get to run an empty Neo4j graph to build a network.(Read PREQUISITES above)
    method_name = "rf" #column names of collections depend on this, i.e. category_afw, centralities_afw, user_features_afw
    mongo_port = "12345" #default 27017
    databaseName = "snol_dataset_final"
    tweetCollection = "col_tweets"
    userCollection = "col_users"
    edgesCollection = "col_edges"
    outputFile = os.path.join(os.getcwd(), 'output', 'col_17M_calculations{}'.format(("_"+method_name) if len(method_name)>0 else "") )
    outputCollection = "col_users_calculations_on_17M{}".format(("_"+method_name) if len(method_name)>0 else "")
    categoryColumnName = "experiment_test.category{}".format(("_"+method_name) if len(method_name)>0 else "")
#    method_name = "afw" #column names of collections depend on this, i.e. category_afw, centralities_afw, user_features_afw
#    mongo_port = "27017" #default 27017
#    databaseName = "snol_dataset_v0"
#    tweetCollection = "col_800k_tweets_labelled_v2"
#    userCollection = "userSet_3665_calculations"
#    edgesCollection = "col_edges"
#    outputFile = os.path.join(os.getcwd(), 'output', 'deneme{}'.format(("_"+method_name) if len(method_name)>0 else "") )
#    outputCollection = "deneme{}".format(("_"+method_name) if len(method_name)>0 else "")
#    categoryColumnName = "category{}".format(("_"+method_name) if len(method_name)>0 else "")

    #Run main method
    myProgram = RunCalculators(mongo_port, databaseName, tweetCollection, userCollection, edgesCollection, outputFile, outputCollection,categoryColumnName,method_name)
    myProgram.main()

