from CentralityMeasures import CentralityMeasures
from UserFeatures import UserFeatures
from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import col, udf, struct
from pyspark.sql.types import DoubleType
from pyspark.ml.feature import MinMaxScaler, StandardScaler, VectorAssembler
from pyspark.ml import Pipeline
from neo4j import GraphDatabase, basic_auth
from pyspark.sql.utils import AnalysisException


class RunCalculators:
    
    def __init__(self, mongo_port, databaseName, tweetCollection, userCollection, edgesCollection, outputFile, outputCollection, categoryColumnName, method_name):
        self.mongo_port = mongo_port
        self.databaseName = databaseName
        self.tweetCollection = tweetCollection
        self.userCollection = userCollection
        self.edgesCollection = edgesCollection
        self.outputFile = outputFile
        self.outputCollection = outputCollection
        self.categoryColumnName = categoryColumnName
        self.method_name = method_name
        self.categories = []
        self.user_features_topics = []
        self.centralities_topics = []
        self.centrality_categories = []
        self.userFeatures_df = None
        self.centralities_df = None
        self.merged_df = None
        self.normalized_df = None
        self.sparkSession = None
        self.neo4jDriver = None
        self.user_df = None
        self.tweet_df = None
        self.edges_df = None
        self.spammers_df = None
        
        
    def main(self):
        #set Spark Session
        self.setSpark()
        
        #connect Neo4j Session
        self.setNeo4j()
        
        #connect MongoDB and set DataFrames
        self.setDataFrames(self.mongo_port, self.databaseName, self.tweetCollection, self.userCollection, self.edgesCollection, self.categoryColumnName)
        
        #exclude spammer users
        print("before spam module count of user_df:{}".format(self.user_df.count()))
        self.excludeSpammers(self.user_df, self.tweet_df, self.edges_df)
        print("after spam module:{}".format(self.user_df.count()))
        
        #run user feature calculator
        myUserFeatures = UserFeatures(self.user_df, self.tweet_df, self.sparkSession, self.method_name)
        myUserFeatures.startCalculation()
        self.userFeatures_df = myUserFeatures.getResults()
        self.categories = myUserFeatures.getCategories()
        self.user_features_topics = myUserFeatures.getUserFeaturesTopics()
        print("after UserFeatures module count of userFeatures_df:{}".format(self.userFeatures_df.count()))
        self.userFeatures_df.printSchema()
        print("categories:",self.categories)

        #run centralities calculator
        myCentralities = CentralityMeasures(self.edges_df, self.userFeatures_df, self.categories, self.sparkSession, self.neo4jDriver, self.method_name)
        myCentralities.insertDataToGraph()  #RUN THIS METHOD ONLY ONCE FOR EVERY GRAPH
        myCentralities.startCalculation()
        self.centralities_df = myCentralities.getResults()
        self.centrality_categories = myCentralities.getCentralityCategories()
        self.centralities_topics = myCentralities.getCentralitiesTopics()
        print("centralities result: ", self.centralities_df.count())
        self.centralities_df.printSchema()
        
        
        #merge, normalize and export dataframes
        self.mergeDataFrames(self.userFeatures_df, self.centralities_df, self.method_name)
        print("after merge: ", self.merged_df.count())
        self.merged_df.printSchema()
        self.normalizeDataFrame(self.merged_df, self.user_features_topics, self.centralities_topics, self.categories, self.centrality_categories, self.method_name)
        print("before exporting normalized_df:", self.normalized_df.count())
        self.normalized_df.printSchema()
        self.exportDataFrame(self.normalized_df, self.outputFile)
        
        #save results to database
        self.saveResultsToDB(self.normalized_df, self.user_df, self.mongo_port, self.databaseName, self.outputCollection)
        
        #stop spark and neo4j
        self.neo4jDriver.session().close()
        self.sparkSession.stop()
        
    def setSpark(self):
        #spark configurations
        conf=SparkConf()
        conf.setMaster("local[4]")          # if you got java memory error, you can reduce running cores
    #    conf.set("spark.executor.memory", "32g")          # this doesn't important when you run on local
        conf.set("spark.driver.memory", "20g")
        conf.set("spark.dirver.maxResultSize", "6g")
        conf.set("spark.memory.offHeap.size","18g")         # you can increase this to overcome java memory issues
    #    conf.set("spark.executor.extraJavaOptions", "-Xmx1024m")
        conf.set("spark.executor.extraJavaOptions","-XX:+UseCompressedOops")
    #    conf.set("spark.cores.max", "2")
    #    conf.set("spark.driver.extraClassPath",
    #             driver_home+'/jdbc/postgresql-9.4-1201-jdbc41.jar:'\
    #             +driver_home+'/jdbc/clickhouse-jdbc-0.1.52.jar:'\
    #             +driver_home+'/mongo/mongo-spark-connector_2.11-2.2.3.jar:'\
    #             +driver_home+'/mongo/mongo-java-driver-3.8.0.jar') 
        sc = SparkContext.getOrCreate(conf)
        self.sparkSession = SparkSession(sc)
        
    def setNeo4j(self):
        driver = GraphDatabase.driver("bolt://localhost", auth=basic_auth("neo4j", "12345678"))
        self.neo4jDriver = driver
        
    def setDataFrames(self, mongo_port, db, tweetColl, userColl, edgesColl, categoryColumnName):
        spark = self.sparkSession
        temp_u_df = spark.read.format("mongo").option("uri","mongodb://127.0.0.1:{}/{}.{}".format(mongo_port,db, userColl)).option('sampleSize', 40000).load()
        #We don't like "id" column :)
        if "id" in temp_u_df.columns:
            temp_u_df = temp_u_df.drop("id")
        self.user_df = temp_u_df.withColumnRenamed("id_str", "id")
        temp_t_df = spark.read.format("mongo").option("uri","mongodb://127.0.0.1:{}/{}.{}".format(mongo_port,db, tweetColl)).option('sampleSize', 50000).load()
        self.tweet_df = temp_t_df.select(col('id_str').alias('tweetId'),col('user.id_str').alias("userId"),col('user.screen_name').alias('screen_name'),col('created_at'),col('{}'.format(categoryColumnName)).alias('category'))
        temp_e_df= spark.read.format("mongo").option("uri","mongodb://127.0.0.1:{}/{}.{}".format(mongo_port,db, edgesColl)).option('sampleSize', 5000).load()
        self.edges_df = temp_e_df.dropDuplicates(["Source","Target"])
        
    def excludeSpammers(self, userdf, tweetdf, edgesdf):
        try:
            self.spammers_df = userdf.filter(userdf['experiment_spam.category_rf']=="1").select(col("id"))
            spammers_list = self.spammers_df.rdd.map(lambda r: r[0]).collect()
            self.tweetdf = tweetdf.filter(~tweetdf.userId.isin(spammers_list))
            self.user_df = userdf.filter(~userdf.id.isin(spammers_list))
            self.edges_df = edgesdf.filter(~edgesdf.Source.isin(spammers_list) & ~edgesdf.Target.isin(spammers_list))
        except AnalysisException:
            pass
        
    def mergeDataFrames(self, userFeatures_df, centralities_df, method_name):
        self.merged_df = centralities_df.join(userFeatures_df,centralities_df.id==userFeatures_df.id)   \
        .select(userFeatures_df["id"],  \
                "user_features{}".format(("_"+method_name) if len(method_name)>0 else ""), \
                "centralities{}".format(("_"+method_name) if len(method_name)>0 else ""))
        
    def exportDataFrame(self, df, outputFile):
        df.coalesce(1).write.format('json').save(outputFile)
        
    def saveResultsToDB(self, results_df, user_df, mongo_port, db, outputCollection):
        user_info_with_features=user_df.join(results_df,user_df.id == results_df.id,how='left').withColumnRenamed("id", "id_str")
        user_info_with_features.write.format("mongo").mode("append").option("uri","mongodb://127.0.0.1:{}/{}.{}".format(mongo_port, db, outputCollection)).save()

    def normalizeDataFrame(self, dataframe, features, centralities, categories, centrality_categories, method_name, method="minmax"):
        # UDF for converting column type from vector to double type
        unlist = udf(lambda x: round(float(list(x)[0]),7), DoubleType())
        # scale "user_features"  
        df=dataframe.select("id")
        for clm in features:
            sub_df=dataframe.select("id")
            for sub_clm in categories:       
                temp=dataframe.select("id","user_features{}.{}.{}".format(("_"+method_name) if len(method_name)>0 else "",clm,sub_clm)).fillna(0)
                # VectorAssembler Transformation - Converting column to vector type
                assembler = VectorAssembler(inputCols=[sub_clm],outputCol=sub_clm+"_Vect")
                if method=="minmax":
                    # MinMaxScaler Transformation
                    scaler = MinMaxScaler(min=0.0,max=1.0,inputCol=sub_clm+"_Vect", outputCol=sub_clm+"_Scaled")
                if method=="std":
                    # StandartScaler Transformation
                    scaler = StandardScaler(inputCol=sub_clm+"_Vect", outputCol=sub_clm+"_Scaled",withStd=True, withMean=False)
                # Pipeline of VectorAssembler and selected scaler
                pipeline = Pipeline(stages=[assembler, scaler])
                # Fitting pipeline on dataframe
                temp = pipeline.fit(temp).transform(temp).withColumn(sub_clm+"_Scaled", unlist(sub_clm+"_Scaled")).drop(sub_clm+"_Vect").drop(sub_clm).withColumnRenamed(sub_clm+"_Scaled",sub_clm)
                # Add scaled columns to sub_df
                sub_df=sub_df.join(temp,"id")
            # Assign categories as sub-columns to feature columns
            sub_df=sub_df.select("id",struct(sub_df.columns[1:]).alias(clm))
            # Add feature columns to df
            df=df.join(sub_df,"id")
        # Assign features as sub-columns to "user_features"
        features_df=df.select("id",struct(df.columns[1:]).alias("user_features{}".format(("_"+method_name) if len(method_name)>0 else "")))
        
        # scale "centralities"
        df=dataframe.select("id")
        for clm in centralities:
            sub_df=dataframe.select("id")
            for sub_clm in centrality_categories:       
                temp=dataframe.select("id","centralities{}.{}.{}".format(("_"+method_name) if len(method_name)>0 else "",clm,sub_clm)).fillna(0)
                # VectorAssembler Transformation - Converting column to vector type
                assembler = VectorAssembler(inputCols=[sub_clm],outputCol=sub_clm+"_Vect")
                # MinMaxScaler Transformation
                scaler = MinMaxScaler(min=0.0,max=1.0,inputCol=sub_clm+"_Vect", outputCol=sub_clm+"_Scaled")
                # Pipeline of VectorAssembler and MinMaxScaler
                pipeline = Pipeline(stages=[assembler, scaler])
                # Fitting pipeline on dataframe
                temp = pipeline.fit(temp).transform(temp).withColumn(sub_clm+"_Scaled", unlist(sub_clm+"_Scaled")).drop(sub_clm+"_Vect").drop(sub_clm).withColumnRenamed(sub_clm+"_Scaled",sub_clm)
                # Add scaled columns to sub_df
                sub_df=sub_df.join(temp,"id")
            # Assign categories as sub-columns to centrality columns
            sub_df=sub_df.select("id",struct(sub_df.columns[1:]).alias(clm))
            # Add centrality columns to df
            df=df.join(sub_df,"id")
        # Assign centralities as sub-columns to "centralities"
        centralities_df=df.select("id",struct(df.columns[1:]).alias("centralities{}".format(("_"+method_name) if len(method_name)>0 else "")))
        # join centralities and user features
        self.normalized_df=features_df.join(centralities_df,"id")   
                
#        def getResultsAsPandas(self):
#            return self.normalized_df.toPandas()
