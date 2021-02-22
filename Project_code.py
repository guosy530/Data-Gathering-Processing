from pyspark import SparkContext
from pyspark.sql import SQLContext

spark = (SparkSession \
        .builder \
        .appName('Project_name') \
        .config('spark.executor.memory', '20G') \
        .config('spark.driver.memory', '20G') \
        .config('spark.driver.maxResultSize', '10G') \
        .config('spark.sql.shuffle.partitions',300) \
        .config('spark.worker.cleanup.enabled', 'True') \
        .config('spark.local.dir','/tmp/spark-temp') \
        .getOrCreate())

# sc= SparkContext(appName="si618_hw6_output_1_shiyuguo")
sqlContext = SQLContext(sc)
gdp_df=sqlContext.read.csv("hdfs://cavium-thunderx/user/shiyuguo/Project1/gdp_d.csv",header=True)
mortality_df=sqlContext.read.csv("hdfs://cavium-thunderx/user/shiyuguo/Project1/mortality_d.csv",header=True)
continent_df=sqlContext.read.csv("hdfs://cavium-thunderx/user/shiyuguo/Project1/countryContinent.csv",header=True)

gdp_df.printSchema()
mortality_df.printSchema()
continent_df.printSchema()

### Reshape data from wide to long
cols1=gdp_df.columns[4:]
gdp_df.withColumnRenamed("Country Name","Country_name") \
    .withColumnRenamed("Country Code","Country_code") \
    .selectExpr('Country_name','Country_code',"stack({0},{1}) as (`Year`,`GDP_percapita`)".format(len(cols1), ','.join(("'{}', `{}`".format(i, i) for i in cols1)))) \
    .registerTempTable('gdp')

cols2=mortality_df.columns[4:]
mortality_df.withColumnRenamed("Country Code","Country_code") \
    .withColumnRenamed("Country Name","Country_name") \
    .selectExpr('Country_name','Country_code',"stack({0},{1}) as (`Year`,`Mortality_rate`)".format(len(cols2), ','.join(("'{}', `{}`".format(i, i) for i in cols2)))) \
    .registerTempTable('mortality')
    
continent_df.registerTempTable('continent')

### Join 3 datasets
merge1 = sqlContext.sql('SELECT g.Country_name, g.Country_code, CAST(g.Year as double), CAST(g.GDP_percapita as double), CAST(m.Mortality_rate as double) \
                        FROM gdp g FULL OUTER JOIN mortality m ON (g.Country_name=m.Country_name AND g.Year=m.Year) \
                        WHERE g.Year !=2020 \
                        ORDER BY g.Country_name,g.Year ASC').registerTempTable('Merge1')

merge2= sqlContext.sql('SELECT m.Country_name, c.continent, c.sub_region,m.Year,m.GDP_percapita as GDP_percapita,(m.Mortality_rate/1000)*100 as Mortalityrate \
                        FROM Merge1 m JOIN continent c ON (m.Country_code=c.code_3 AND m.Country_name=c.country) \
                        WHERE m.Year !=2020 \
                        ORDER BY m.Country_name,m.Year ASC')
merg2rdd= merge2.rdd.map(lambda i: ','.join(str(j) for j in i)).saveAsTextFile('project1_final_merge')
merge2.registerTempTable('Merge2')

###Q1: World and Continent Huamn progress trends
world_ratio= sqlContext.sql('SELECT Year, round(avg(Mortalityrate),2),round(avg(GDP_percapita),2) \
                             FROM Merge2 WHERE Year <=2015 GROUP BY Year ORDER BY Year ASC')

world_ratiordd= world_ratio.rdd.map(lambda i: ','.join(str(j) for j in i)).saveAsTextFile('project1_world_ratio')


continent_ratio = sqlContext.sql('SELECT continent,Year, round(avg(Mortalityrate),2),round(avg(GDP_percapita),2) \
                                  FROM Merge2 WHERE Year<=2015 GROUP BY continent,Year ORDER BY continent,Year ASC')                     

continent_ratiordd= continent_ratio.rdd.map(lambda i: ','.join(str(j) for j in i)).saveAsTextFile('project1_continent_ratio')

### Q2: The performance of countries by regions before and after 2015
difference = sqlContext.sql('SELECT b.Country_name,b.continent,b.sub_region,b.Year,b.Mortalityrate-a.Mortalityrate AS Mortality_diff, ((b.GDP_percapita-a.GDP_percapita)/a.GDP_percapita)*100 AS GDPPer_diff \
                             FROM Merge2 a JOIN Merge2 b on b.Country_name=a.Country_name AND (b.Year-1)=a.Year \
                             ORDER BY b.Country_name,b.Year ASC')
differencerdd= difference.rdd.map(lambda i: ','.join(str(j) for j in i)).saveAsTextFile('project1_merge_diff')

difference.registerTempTable('Diff') 
pre_subregion_ratio= sqlContext.sql('SELECT continent, sub_region,round(avg(Mortality_diff/GDPPer_diff),4) AS pre_ratio \
                       FROM Diff WHERE Year <= 2015 \
                       GROUP BY continent,sub_region ORDER BY continent,sub_region DESC').registerTempTable('pre_subregion')

post_subregion_ratio=sqlContext.sql('SELECT continent, sub_region,round(avg(Mortality_diff/GDPPer_diff),4) AS post_ratio \
                       FROM Diff WHERE Year > 2015 \
                       GROUP BY continent,sub_region ORDER BY continent,sub_region DESC').registerTempTable('post_subregion')

subregion_ratio = sqlContext.sql('SELECT b.continent,b.sub_region,a.pre_ratio, b.post_ratio,ROUND(b.post_ratio-a.pre_ratio,4) AS ratio_compare \
                       FROM post_subregion b JOIN pre_subregion a ON b.continent=a.continent AND b.sub_region=a.sub_region \
                       ORDER BY continent,ratio_compare DESC')

subregion_ratiordd= subregion_ratio.rdd.map(lambda i: ','.join(str(j) for j in i)).saveAsTextFile('project1_subregion_ratio')
# subregion_ratio.write.format('csv').option('delimiter',',').option('header','true').save('project1_subregion_ratio')

#### 3. top 20 well performanced countries pre-2015 and post-2015
pre_country_ratio= sqlContext.sql('SELECT Country_name,continent,round(avg(Mortality_diff/GDPPer_diff),3) AS pre_ratio \
                       FROM Diff WHERE Year <= 2015 \
                       GROUP BY Country_name,continent ORDER BY pre_ratio DESC').registerTempTable('pre_country')

post_country_ratio= sqlContext.sql('SELECT Country_name, continent, round(avg(Mortality_diff/GDPPer_diff),3) AS post_ratio \
                       FROM Diff WHERE Year > 2015 \
                       GROUP BY Country_name,continent ORDER BY post_ratio DESC').registerTempTable('post_country')

pre_rank= sqlContext.sql('SELECT * FROM pre_country LIMIT 20').show()

post_rank= sqlContext.sql('SELECT * FROM post_country LIMIT 20').show()

compare_rank= sqlContext.sql('SELECT b.Country_name,b.continent, ROUND(b.post_ratio-a.pre_ratio,3) AS ratio_compare \
                       FROM post_country b JOIN pre_country a ON b.Country_name=a.Country_name\
                       ORDER BY ratio_compare DESC LIMIT 20') .show()

