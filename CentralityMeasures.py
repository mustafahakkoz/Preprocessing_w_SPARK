from pyspark.sql.functions import col, when, explode, struct, array, lit, array_remove,array_intersect

class CentralityMeasures:
    
    def __init__(self, edges_df, user_features_df, categories, sparkSession, neo4jDrvier, method_name):
        self.edges_df = edges_df
        self.user_features_df = user_features_df
        self.list_categories = categories
        self.sparkSession = sparkSession
        self.neo4jDriver = neo4jDrvier
        self.method_name = method_name
        self.nodes_df = None
        self.results_df = None 
        self.centrality_categories = []

            
    ###(RUN THIS METHOD ONCE)###
    def insertDataToGraph(self):
        spark = self.sparkSession
        neo4j = self.neo4jDriver.session()
        sc = spark.sparkContext
        feats = self.user_features_df
        list_cat=self.list_categories
        cat_count=len(list_cat)

        
        #import edges
        e = self.edges_df
        self.nodes_df = e.select("Source").union(e.select("Target")).distinct().withColumnRenamed('Source', 'id')
        n = self.nodes_df
        print(feats.count(), list_cat, e.count(), n.count())
        feats.printSchema()
                
        #cache dataframes        
        feats.cache()
        e.cache()
        n.cache()
        
        #add category property to u
        u_focus_rate=feats.select(col('id'),col("user_features{}.dict_focus_rate".format(("_"+self.method_name) if len(self.method_name)>0 else "")).alias("dict_focus_rate"))
        u_with_category=u_focus_rate.withColumn("userCategory",array([lit(c) for c in list_cat]))
        for cat in list_cat:
            u_with_category=u_with_category.withColumn("temp", when(col("dict_focus_rate.{}".format(cat))<1/cat_count, array_remove(u_with_category["userCategory"], cat)).otherwise(u_with_category["userCategory"])).drop("userCategory").withColumnRenamed("temp","userCategory")
        u_with_category=u_with_category.select("id","userCategory")   
        
        #join n and u_with_category
        n_with_category=n.join(u_with_category,"id",how="left")
        
        #add category columns to e
        e_with_category=e.join(n_with_category,e.Source==n_with_category.id,how="left").withColumnRenamed("userCategory","sourceCategory").select("Source","Target","sourceCategory")
        e_with_category=e_with_category.join(n_with_category,e_with_category.Target==n_with_category.id,how="left").withColumnRenamed("userCategory","targetCategory").select("Source","Target","sourceCategory","targetCategory")
        
        #determine intersection between sourceCategory and targetCategory
        e_with_category=e_with_category.withColumn("Categories", array_intersect(e_with_category["sourceCategory"],e_with_category["targetCategory"]))
        
        #flatten out categories of edges
        e_with_category=e_with_category.withColumn("Category",explode(col("Categories"))).select("Source","Target","Category")
        print("e_with_category", e_with_category.count())
        e_with_category.printSchema()
        
        ## Insert data 
        insert_query = '''
        UNWIND {triples} as triple
        MERGE (p1:User {id:triple[0]})
        MERGE (p2:User {id:triple[1]}) WITH p1,p2,triple
        CALL apoc.create.relationship(p1, triple[2], {}, p2) YIELD rel
        RETURN *
        '''
        
        e_listoftriples=e_with_category.toPandas()[['Source','Target','Category']].values.tolist()
        print("e_listoftriples:", len(e_listoftriples))
        batches = list(self.generate_batches(e_listoftriples, 7000))
        for batch in batches:
            neo4j.run(insert_query, parameters={"triples": batch})
                
        e_with_category.show()
        print("batches size:", len(batches), " last batch:",  len(batches[-1]))
        
        
        #clear cache
#        spark.catalog.clearCache()
        
        
    def startCalculation(self):
        spark = self.sparkSession
        neo4j = self.neo4jDriver.session()
        sc = spark.sparkContext
        n = self.nodes_df
        
        #cache dataframes        
        n.cache()
        
        #get relationship list (some categories would be empty)
        rel_query = '''
        MATCH ()-[relationship]->() 
        RETURN DISTINCT TYPE(relationship) AS type
        ORDER BY type ASC;
        '''
        results = neo4j.run(rel_query)
        temp_df=spark.createDataFrame(results)
        list_cat_updated = temp_df.select("_1").rdd.flatMap(lambda x: x).collect()
        self.centrality_categories = list_cat_updated
        
        #Degree Centrality
        for cat in list_cat_updated:
            degreeCentrality_query = '''
            CALL algo.degree.stream("User", "%s", {write: true, direction: "both"})
            YIELD nodeId, score as degreeCentrality
            RETURN algo.getNodeById(nodeId).id as userId, degreeCentrality
            ORDER BY userId DESC
            '''%(cat)
            results = neo4j.run(degreeCentrality_query)
            temp_df=spark.createDataFrame(results).withColumnRenamed("_1", "userId").withColumnRenamed("_2", "degreeCentrality")
            n=n.join(temp_df, n.id==temp_df.userId, how="left").withColumnRenamed("degreeCentrality",cat)
        n=n.select("id", struct(list_cat_updated).alias("degreeCentrality"))
        
        #Closeness Centrality
        for cat in list_cat_updated:
            closenessCentrality_query = '''
            CALL algo.closeness.stream("User", "%s",{write: true})
            YIELD nodeId, centrality as closenessCentrality
            RETURN algo.getNodeById(nodeId).id as userId, closenessCentrality
            ORDER BY userId DESC
            '''%(cat)
            results = neo4j.run(closenessCentrality_query)
            temp_df=spark.createDataFrame(results).withColumnRenamed("_1", "userId").withColumnRenamed("_2", "closenessCentrality")
            n=n.join(temp_df, n.id==temp_df.userId, how="left").withColumnRenamed("closenessCentrality",cat)
        n=n.select("id", "degreeCentrality",struct(list_cat_updated).alias("closenessCentrality"))
        
        #Betweenness Centrality
        for cat in list_cat_updated:
            betweennessCentrality_query = '''
            CALL algo.betweenness.stream("User", "%s",{write: true})
            YIELD nodeId, centrality as betweennessCentrality
            RETURN algo.getNodeById(nodeId).id as userId, betweennessCentrality
            ORDER BY userId DESC
            '''%(cat)
            #(USE THIS FOR LARGER GRAPHS)
            #Approximation of Betweenness Centrality(RA-Brandes algorithm) 
            #aproxBetweennessCentrality_query = '''
            #CALL algo.betweenness.sampled.stream("User", "%s", {strategy:"degree"})
            #YIELD nodeId, centrality as aproxBetweennessCentrality
            #RETURN algo.getNodeById(nodeId).id AS userId, aproxBetweennessCentrality
            #ORDER BY userId DESC
            #'''%(cat)
            results = neo4j.run(betweennessCentrality_query)
            temp_df=spark.createDataFrame(results).withColumnRenamed("_1", "userId").withColumnRenamed("_2", "betweennessCentrality")
            n=n.join(temp_df, n.id==temp_df.userId, how="left").withColumnRenamed("betweennessCentrality",cat)
        n=n.select("id", "degreeCentrality","closenessCentrality",struct(list_cat_updated).alias("betweennessCentrality"))    
    
        #PageRank
        for cat in list_cat_updated:
            pageRank_query = '''
            CALL algo.pageRank.stream('User', '%s', {iterations:100, dampingFactor:0.85, write: true})
            YIELD nodeId, score as pageRank
            RETURN algo.getNodeById(nodeId).id AS userId, pageRank
            ORDER BY userId DESC
            '''%(cat)
            results = neo4j.run(pageRank_query)
            temp_df=spark.createDataFrame(results).withColumnRenamed("_1", "userId").withColumnRenamed("_2", "pageRank")
            n=n.join(temp_df, n.id==temp_df.userId, how="left").withColumnRenamed("pageRank",cat)
        n=n.select("id", "degreeCentrality","closenessCentrality","betweennessCentrality", struct(list_cat_updated).alias("pageRank"))    
        
        #set results
        self.results_df = n.select("id",struct(n.columns[1:]).alias("centralities{}".format(("_"+self.method_name) if len(self.method_name)>0 else "")))
        
        #clear cache
#        spark.catalog.clearCache()
        
    def generate_batches(self, l, n):
        # For item i in a range that is a length of l,
        for i in range(0, len(l), n):
            # Create an index range for l of n items:
            yield l[i:i+n]
        
    def getResults(self):
        return self.results_df
    
    def getCentralitiesTopics(self):
        return ['betweennessCentrality','closenessCentrality','degreeCentrality','pageRank'] 
    
    def getCentralityCategories(self):
        return self.centrality_categories 