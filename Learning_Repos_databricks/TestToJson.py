# Databricks notebook source
import json
import re

def usageToJson(text):
  jsonStr = re.sub("((?=\D)\w+)=", r'"\1":',  text)  
  jsonStr = re.sub(":([\w\s\-@_\./();!><,]+)", r':"\1"',  jsonStr)  
  jsonStr = re.sub(':(?=\D)([{,:])(\w+)([},:])',':\\1\"\\2\"\\3',jsonStr)  
  jsonStr = re.sub("[0-9]\":\"([0-9])", r':\1',  jsonStr)  
  jsonStr = re.sub("\",([\w\s\-@_\.]+):", r'\","\1":',  jsonStr)  
  jsonStr = re.sub(", \"", '", ',  jsonStr) 
  jsonStr = re.sub(",\"\"", '",\"' , jsonStr) 
  jsonStr = re.sub("\"{\"", '{\"',  jsonStr) 
  jsonStr = re.sub("\"}\", ", '\"}, \"',  jsonStr) 
  jsonStr = re.sub("\"}\"},", '\"}},',  jsonStr) 
  jsonStr = re.sub("\"}\",", '\"},',  jsonStr) 
  return jsonStr

spark.udf.register("usageToJson", usageToJson)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT properties ,usageToJson(properties) as prop,get_json_object(usageToJson(properties),'$.userAgent'),get_json_object(usageToJson(properties),'$.requestId')
# MAGIC FROM logs.databricksaudit
# MAGIC limit 10

# COMMAND ----------

# MAGIC %sql
# MAGIC select *
# MAGIC from logs.databricksaudit
# MAGIC where properties like '%Mozilla/5.0 (Windows NT 10.0; Win64; x64%'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT case 
# MAGIC   when properties like '%Apache-HttpClient/4.5.4 (Java/1.8.0_265)%' then 'Apache-HttpClient/4.5.4 (Java/1.8.0_265)' 
# MAGIC   when properties like '%AzureDataFactory%' then 'AzureDataFactory' 
# MAGIC   when properties like '%Mozilla/5.0 (Windows NT 10.0; Win64; x64%' then 'Mozilla/5.0 (Windows NT 10.0; Win64; x64'   else 'other' end
# MAGIC   as api,count(*) as counter
# MAGIC FROM logs.databricksaudit
# MAGIC group by case 
# MAGIC   when properties like '%Apache-HttpClient/4.5.4 (Java/1.8.0_265)%' then 'Apache-HttpClient/4.5.4 (Java/1.8.0_265)' 
# MAGIC   when properties like '%AzureDataFactory%' then 'AzureDataFactory' 
# MAGIC   when properties like '%Mozilla/5.0 (Windows NT 10.0; Win64; x64%' then 'Mozilla/5.0 (Windows NT 10.0; Win64; x64'   else 'other' end

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT properties
# MAGIC FROM logs.databricksaudit
# MAGIC where get_json_object(usageToJson(properties),'$.userAgent') is null
# MAGIC   and properties not like '%userAgent=null%'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT get_json_object(usageToJson(properties),'$.userAgent') as api,count(*)
# MAGIC FROM logs.databricksaudit
# MAGIC group by get_json_object(usageToJson(properties),'$.userAgent')

# COMMAND ----------


