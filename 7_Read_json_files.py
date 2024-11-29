# Databricks notebook source
spark

# COMMAND ----------


# readingdelimited json

json_read=spark.read.format("json")\
    .option("inferschema","true")\
    .option("mode","PERMISSIVE")\
    .load("/FileStore/tables/line_delimited_json.json")

json_read.show()

# COMMAND ----------


# reading single_file_json_with_extra_fields

json_read=spark.read.format("json")\
    .option("inferschema","true")\
    .option("mode","PERMISSIVE")\
    .load("/FileStore/tables/single_file_json_with_extra_fields.json")

json_read.show()

# COMMAND ----------


#Reading Multi_line_correct json

json_read=spark.read.format("json")\
    .option("inferschema","true")\
    .option("mode","PERMISSIVE")\
    .option("multiline","true")\
    .load("/FileStore/tables/Multi_line_correct-2.json")

json_read.show()

# COMMAND ----------

# reading multiline incorreect json


json_read=spark.read.format("json")\
    .option("inferschema","true")\
    .option("mode","PERMISSIVE")\
    .load("/FileStore/tables/Multi_line_incorrect.json")

json_read.show()

# COMMAND ----------

# reading corrupted multiline json---failed
json_read=spark.read.format("json")\
    .option("inferschema","true")\
    .option("mode","PERMISSIVE")\
    .load("/FileStore/tables/corrupt_Multi_linejson.json")

json_read.show()



# COMMAND ----------

# reading singleline corrupted json ...corrected automatically
json_read=spark.read.format("json")\
    .option("inferschema","true")\
    .option("mode","PERMISSIVE")\
    .load("/FileStore/tables/corrupted_json.json")

json_read.show()





# COMMAND ----------


# reading singleline corrupted json ...corrected automatically..truncate=False
json_read=spark.read.format("json")\
    .option("inferschema","true")\
    .option("mode","PERMISSIVE")\
    .load("/FileStore/tables/corrupted_json.json")

json_read.show(truncate=False)





# COMMAND ----------

# reading Nested json...
json_read=spark.read.format("json")\
    .option("inferschema","true")\
    .option("mode","PERMISSIVE")\
    .load("/FileStore/tables/file5.json")

json_read.show()





# COMMAND ----------

# reading Nested json....(printSchema())
json_read=spark.read.format("json")\
    .option("inferschema","true")\
    .option("mode","PERMISSIVE")\
    .load("/FileStore/tables/file5.json")

json_read.printSchema()





# COMMAND ----------


