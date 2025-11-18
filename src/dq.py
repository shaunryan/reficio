# Databricks notebook source
# MAGIC %sql
# MAGIC
# MAGIC create catalog if not exists unified;
# MAGIC create schema if not exists system;

# COMMAND ----------

# MAGIC %sql
# MAGIC use catalog unified;
# MAGIC use schema system;

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists system.rule;
# MAGIC create table system.rule(
# MAGIC     `id`            bigint generated always as identity,
# MAGIC     `dataset`       string    not null comment 'Name of the dataset to apply the rule',
# MAGIC     `column`        string    not null comment 'Name of the column to apply the rule',
# MAGIC     `description`   string    not null comment 'A human readable description of rule and why it''s being applied',
# MAGIC     `class`         string    not null comment '''validation'' that evaluates to true or false or ''cleanse'' transforms a value to another value',
# MAGIC     `type`          string    not null comment 'Expression, replacement, reference set',
# MAGIC     `function`      string    not null comment 'The function to apply to the column',
# MAGIC     `parameters`    string             comment 'Arguments to pass into the function, supports using {column} and {relacement} variables',
# MAGIC     `replacement`   string             comment 'A replacement value',
# MAGIC     `cast_as`       string    not null comment 'Cast the cleanse transform to a specific type',
# MAGIC     `order`         string    not null comment 'The order in wich to apply the rule for the table and column',
# MAGIC     `created_by`    string    not null comment 'The user who created the rule',
# MAGIC     `created_at`    timestamp not null comment 'The time the rule was created'
# MAGIC );
# MAGIC insert into system.rule (     
# MAGIC `dataset`    ,   
# MAGIC `column`     ,   
# MAGIC `description`,   
# MAGIC `class`      ,   
# MAGIC `type`       ,   
# MAGIC `function`   ,   
# MAGIC `parameters` ,   
# MAGIC `replacement`,   
# MAGIC `cast_as`    ,   
# MAGIC `order`      ,   
# MAGIC `created_by` ,   
# MAGIC `created_at`    
# MAGIC )
# MAGIC values
# MAGIC (
# MAGIC 'test', 'value', 'replace all whitespace', 'cleanse', 'function', 'regexp_replace', '{column}, \'\\\\s+\', \'{replacement}\'', ' ', 'string', 0, current_user(), now()
# MAGIC ), (
# MAGIC 'test', 'value', 'trim whitespace', 'cleanse', 'function', 'trim', '{column}', null, 'string', 1, current_user(), now() 
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC create schema if not exists `test`;
# MAGIC drop table if exists `test`.`rule`;
# MAGIC create table if not exists `test`.`rule`(
# MAGIC   `id`  bigint generated always as identity,
# MAGIC   `value` string,
# MAGIC   `expected_value` string,
# MAGIC   `expected_type` string
# MAGIC );
# MAGIC
# MAGIC insert into `test`.`rule` (`value`, `expected_value`, `expected_type`)
# MAGIC values 
# MAGIC ('           stuff  stuff    stuff \n stuff', 'stuff stuff stuff stuff', 'string')

# COMMAND ----------

from pyspark.sql import functions as fn
from pyspark.sql import DataFrame

def cleanse_rule(rule:dict):
  variables = ['column', 'replacement']
  parameters = rule.get("parameters", "") 
  parameters = "" if parameters is None else parameters
  if parameters:
    for v in variables:
      var = "{" + v + "}"
      val = rule.get(v, "")
      val = "" if val is None else val
      parameters = parameters.replace(var, val)
    parameters = f"({parameters})"
  else:
    parameters = "()"
  r = {
    "function": f"{rule['function']}{parameters}",
    "cast_as": rule["cast_as"],
    "description": rule["description"]
  }
  return r
  
def apply_cleanse(
  df:DataFrame, 
  dataset:str,
  enable_pre_cleansed:bool=False
):

  df_rules = spark.sql("""
    select *
    from system.rule
    where dataset = {dataset}
    and class = 'cleanse'
    order by `column`, `order`               
  """, dataset=dataset).collect()
  
  rules = [rule.asDict() for rule in df_rules]

  column_rules = {}
  for rule in rules:
    if not column_rules.get(rule['column']):
      column_rules[rule['column']] = {}
    column_rules[rule['column']][rule["order"]] = cleanse_rule(rule)

  for column in df.columns:
    if column in column_rules:
      if enable_pre_cleansed:
        df = df.withColumn(f"_pre_cleansed_{column}", fn.col(column))
      for rule_order, rule in column_rules[column].items():
        print(f"Applying rule {rule_order}:{rule['description']} using function:{rule['function']} to dataset column {dataset}.{column} casting as {rule['cast_as']}")
        df = df.withColumn(column, fn.expr(f"cast({rule['function']} as {rule['cast_as']})"))

  return df





# COMMAND ----------



df = spark.sql("""
  select *
  from test.rule
""")

df = apply_cleanse(
  df, 
  dataset='test', 
  enable_pre_cleansed=True
) 

df = (df
  .withColumn("succeeded", fn.expr("value == expected_value"))
)

df.display()