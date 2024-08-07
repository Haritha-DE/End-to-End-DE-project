# Databricks notebook source
# MAGIC %md
# MAGIC ##Doing transformation for all tables

# COMMAND ----------

table_name=[]
for i in dbutils.fs.ls("/mnt/silver/SalesLT/") :
    table_name.append(i.name.split("/")[0])

# COMMAND ----------

table_name

# COMMAND ----------

# DBTITLE 1,Converting Column Name to Coumn_Name
for i in table_name:
    path='/mnt/silver/SalesLT/'+ i
    print(path)
    df=spark.read.format("delta").load(path)

    #Get the list of column names
    column_names=df.columns

    for old_column_name in column_names:
        #converting ColumnName to Column_Name
        new_column_name="".join(["_" + char if char.isupper() and not old_column_name[j-1].isupper() else char for j,char in enumerate(old_column_name)]).lstrip("_")
        #change the column name with columnrenamed
        df=df.withColumnRenamed(old_column_name,new_column_name)
    output_path='/mnt/gold/SalesLT/'+ i + '/'
    df.write.format('delta').mode('overwrite').save(output_path)
        

# COMMAND ----------

df.display()
