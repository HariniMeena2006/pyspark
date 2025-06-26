

df_json=spark.read.format('json').option('inferSchema',True)\
    .option('header',True)\
        .option('multiline',False)\
            .load('/Volumes/bigdata/pyspark_bigdata/bigdata/drivers.json')




df_json.display()



df=spark.read.format('csv').option('inferSchema',True).option('header',True).load('/Volumes/bigdata/pyspark_bigdata/bigdata/')


df.show()

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC Schema Definition
# MAGIC

# COMMAND ----------

df=spark.read.format('csv').option('inferSchema',True).option('header',True).load('/Volumes/bigdata/pyspark_bigdata/bigdata/')

# COMMAND ----------

df.printSchema()

# COMMAND ----------

my_ddl_schema = '''\
Item_Identifier STRING,
Item_Weight STRING,
Item_Fat_Content STRING,
Item_Visibility DOUBLE,
Item_Type STRING,
Item_MRP DOUBLE,
Outlet_Identifier STRING,
Outlet_Establishment_Year INT,
Outlet_Size STRING,
Outlet_Location_Type STRING,
Outlet_Type STRING,
Item_Outlet_Sales DOUBLE
'''



# COMMAND ----------

df=spark.read.format('csv')\
    .schema(my_ddl_schema)\
    .option('header',True)\
    .load('/Volumes/bigdata/pyspark_bigdata/bigdata/')

# COMMAND ----------

df.display()

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### StructType()Schema

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

my_struct_schema=StructType([
    StructField("Item_Identifier", StringType(), True),
    StructField("Item_Weight", DoubleType(), True),
    StructField("Item_Fat_Content", StringType(), True),
    StructField("Item_Visibility", DoubleType(), True),
    StructField("Item_Type", StringType(), True),
    StructField("Item_MRP", DoubleType(), True),
    StructField("Outlet_Identifier", StringType(), True),
    StructField("Outlet_Establishment_Year", IntegerType(), True),
    StructField("Outlet_Size", StringType(), True),
    StructField("Outlet_Location_Type", StringType(), True),
    StructField("Outlet_Type", StringType(), True),
    StructField("Item_Outlet_Sales", DoubleType(), True)
])

# COMMAND ----------

df=spark.read.format('csv')\
    .schema(my_struct_schema)\
    .option('header',True)\
    .load('/Volumes/bigdata/pyspark_bigdata/bigdata/')

# COMMAND ----------

df.display()

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC Select

# COMMAND ----------

df_sel=df.select('Item_Identifier','Item_Weight','Item_Fat_Content').display()

# COMMAND ----------

df_sel=df.select(col('Item_Identifier'),col('Item_Weight'),
                     col('Item_Fat_Content')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ALIAS

# COMMAND ----------

df_sel=df.select(col('Item_Identifier').alias('Item_ID')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Scenerio-1 (Filter)

# COMMAND ----------

df.filter(col('Item_Fat_Content')=='Regular').display()

# COMMAND ----------

# MAGIC %md ###Scenerio-2

# COMMAND ----------

my_struct_schema=StructType([
    StructField("Item_Identifier", StringType(), True),
    StructField("Item_Weight", DoubleType(), True),
    StructField("Item_Fat_Content", StringType(), True),
    StructField("Item_Visibility", DoubleType(), True),
    StructField("Item_Type", StringType(), True),
    StructField("Item_MRP", DoubleType(), True),
    StructField("Outlet_Identifier", StringType(), True),
    StructField("Outlet_Establishment_Year", IntegerType(), True),
    StructField("Outlet_Size", StringType(), True),
    StructField("Outlet_Location_Type", StringType(), True),
    StructField("Outlet_Type", StringType(), True),
    StructField("Item_Outlet_Sales", DoubleType(), True)
])

# COMMAND ----------

df=spark.read.format('csv')\
    .schema(my_struct_schema)\
    .option('header',True)\
    .load('/Volumes/bigdata/pyspark_bigdata/bigdata/')

# COMMAND ----------

df.filter((col('Item_Type')=='Soft Drinks')&
          (col("Item_Weight")<10)).display()

# COMMAND ----------

# MAGIC %md ## Scenerio-3

# COMMAND ----------

from pyspark.sql.functions import col

df.filter(
    (col('Outlet_Size').isNull()) & 
    (col('Outlet_Location_Type') != "Tier 3")
).display()


# COMMAND ----------

# MAGIC %md
# MAGIC ###With column rename

# COMMAND ----------

df = df.withColumnRenamed("Item_Fat_Content", "ItemFat")


# COMMAND ----------

# MAGIC %md
# MAGIC ### Withcolumn
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ###Scenerio-1
# MAGIC

# COMMAND ----------

df=df.withColumn("number",lit("new"))

# COMMAND ----------

df.display()

# COMMAND ----------

from pyspark.sql.functions import col

df = spark.read \
    .format("csv") \
    .option("header", True) \
    .load('/Volumes/bigdata/pyspark_bigdata/bigdata/')


# COMMAND ----------

df.display()  


# COMMAND ----------

df = df.withColumnRenamed("Item_Fat_Content", "ItemFat")

# COMMAND ----------

df.display()

# COMMAND ----------

df=df.withColumn("number",lit("new"))

# COMMAND ----------

df.display()

# COMMAND ----------

df = df.withColumn(
    'multiply',
    col('Item_Weight') * col('Item_MRP')
)

df.display()


# COMMAND ----------

my_struct_schema=StructType([
    StructField("Item_Identifier", StringType(), True),
    StructField("Item_Weight", DoubleType(), True),
    StructField("ItemFat", StringType(), True),
    StructField("Item_Visibility", DoubleType(), True),
    StructField("Item_Type", StringType(), True),
    StructField("Item_MRP", DoubleType(), True),
    StructField("Outlet_Identifier", StringType(), True),
    StructField("Outlet_Establishment_Year", IntegerType(), True),
    StructField("Outlet_Size", StringType(), True),
    StructField("Outlet_Location_Type", StringType(), True),
    StructField("Outlet_Type", StringType(), True),
    StructField("Item_Outlet_Sales", DoubleType(), True)
])

# COMMAND ----------

df=spark.read.format('csv')\
    .schema(my_struct_schema)\
    .option('header',True)\
    .load('/Volumes/bigdata/pyspark_bigdata/bigdata/')

# COMMAND ----------

df = df.withColumn(
    'multiply',
    col('Item_Weight') * col('Item_MRP')
)

df.display()


# COMMAND ----------

# MAGIC %md
# MAGIC ### scenerio-2
# MAGIC
# MAGIC

# COMMAND ----------

df=df.withColumn('ItemFat',regexp_replace(col("ItemFat"),"Regular","Reg")).display()

# COMMAND ----------

from pyspark.sql.functions import regexp_replace, col

df = df.withColumn(
    "ItemFat",
    regexp_replace(col("ItemFat"), "Low Fat", "LF")
)

df.display()



# COMMAND ----------

# MAGIC %md ###Type_casting

# COMMAND ----------

df=df.withColumn('Item_Weight',col('Item_Weight').cast(StringType()))

# COMMAND ----------

df.display()


# COMMAND ----------

df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC Scenerio-1
# MAGIC

# COMMAND ----------

df.sort(col('Item_Weight').desc())
df.display()

# COMMAND ----------

# MAGIC %md Scenerio-2
# MAGIC

# COMMAND ----------

df=df.sort(col('Item_Visibility').asc())
df.display()



# COMMAND ----------

from pyspark.sql.functions import col

df = spark.read \
    .format("csv") \
    .option("header", True) \
    .load('/Volumes/bigdata/pyspark_bigdata/bigdata/')

# COMMAND ----------

# MAGIC %md
# MAGIC ###scenerio-4
# MAGIC

# COMMAND ----------

df.sort([col('Item_Weight'),col('Item_Visibility')],ascending = [0,1]).display()

# COMMAND ----------

# MAGIC %md
# MAGIC Limit

# COMMAND ----------

df.limit(10).display()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ###Drop

# COMMAND ----------

df=df.drop('Item_Visibility').display()

# COMMAND ----------

# MAGIC %md ###Scenerio-2

# COMMAND ----------

df.drop('Item_Type').display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###drop_duplicates

# COMMAND ----------

df.dropDuplicates().display()

# COMMAND ----------

df.drop_duplicates(subset=['Item_Type']).display()

# COMMAND ----------

data1=[('1','harini'),('2','lavanya')]
schema1='id STRING,name STRING'
df1=spark.createDataFrame(data1,schema1)
data2=[('1','sanjay'),('2','harini')]
schema2='id STRING,name STRING'
df2=spark.createDataFrame(data2,schema2)


# COMMAND ----------

df1.display()

# COMMAND ----------

df2.display()

# COMMAND ----------

df1.union(df2).display()


# COMMAND ----------

df1.unionByName(df2).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###String Functions

# COMMAND ----------

from pyspark.sql.functions import lower, col


df1.select(lower(col("name"))).display()


# COMMAND ----------

