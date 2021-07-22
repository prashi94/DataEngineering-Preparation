from pyspark.sql.functions import *
def genrate_rowid(df,Dimension_ID,column):
    windowSpec  = Window.orderBy(column)
    result_df=df.withColumn(Dimension_ID,row_number().over(windowSpec))
    return result_df

def max_id(df,Dimension_ID,column):
    Col_ID = Dimension_ID
    Max_ID_Value=df.agg({Col_ID: "max"}).collect()[0][0]
    print(Max_ID_Value)
    return Max_ID_Value

def merge_fun(Target_Path,DeltaDF,UpdateDict,InsertDict,Join_Column):

    Target_Delta_DF=DeltaTable.forPath(spark,Target_Path)
    if(len(Join_Column)>1):
        var='target.'+Join_Column[0]+' = '+'delta.'+Join_Column[0]
        for i in range(1,len(Join_Column)):
            var+=' AND '+'target.'+Join_Column[i]+' = '+'delta.'+Join_Column[i] 
    else:
        Join_Column=Join_Column[0]
        target_col = 'target.'+Join_Column
        delta_col = 'delta.'+Join_Column
        var = target_col+' = '+delta_col
    print(var)
    Target_Delta_DF.alias("target") \
    .merge(DeltaDF.alias("delta"),condition = expr(var)) \
    .whenMatchedUpdate(set = UpdateDict) \
    .whenNotMatchedInsert(values =InsertDict)\
    .execute()
    print('Upsert logic Completed on Dimension table')

def Dimension_Col_Update_Set(Dimension_Col,Dimension_ID,Join_Column):
    Dimension_Set={}
    for i in range(0,len(Dimension_Col)):
        if((Dimension_Col[i] not in ('Created_By','Created_Date',Dimension_ID,Join_Column))):
            var='delta.'+Dimension_Col[i]
            Dimension_Set[Dimension_Col[i]]=var
    return Dimension_Set

---------------------------------------------------
from delta.tables import *
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number,trim,when,col,current_timestamp,lit,expr

jdbcHostname = dbutils.secrets.get(scope = "coreadbsecret",key = "sqlservername")
jdbcDatabase = dbutils.secrets.get(scope = "coreadbsecret",key = "sqldatabase")
jdbcUsername = dbutils.secrets.get(scope = "coreadbsecret",key = "sqluser")
jdbcPassword = dbutils.secrets.get(scope = "coreadbsecret",key = "sqladminpassword")
jdbcPort = dbutils.secrets.get(scope = "coreadbsecret",key = "sqlport")
jdbcUrl = "jdbc:sqlserver://{0}:{1};database={2};user={3};password={4}".format(jdbcHostname, jdbcPort, jdbcDatabase, jdbcUsername, jdbcPassword)
connectionProperties = {
    "user" : jdbcUsername,
    "password" : jdbcPassword,
    "driver" : "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

def dimension_load():
  
  Dimension_Col_Query = "(select * from [Metadata].[Business_Temp_Metadata] ) dimension_file"
  Dimension_Details = spark.read.jdbc(url=jdbcUrl, table=Dimension_Col_Query, properties=connectionProperties)
  
  Table_Names = Dimension_Details.select("Table_Name").rdd.map(lambda row : row[0]).collect()
  
  for i in range(0,len(Table_Names)):
    Source_Path = Dimension_Details.collect()[i]['Source_Path']
    Target_Path = Dimension_Details.collect()[i]['Target_Path']
    Join_Column = Dimension_Details.collect()[i]['Join_Column'][1:][:-1].split(",")
    Dimension_Col=Dimension_Details.collect()[i]['Column_Details'][1:][:-1].split(",")
    Dimension_ID = Dimension_Col[0]
   
    
    Input_DF = spark.read.format("delta").load(Source_Path)
    for j in range(0, len(Join_Column)):
      Notnull_Source_DF=Input_DF.filter(col(Join_Column[j]).isNotNull())
      
      
    Source_DF=Notnull_Source_DF.distinct()

    
    if (DeltaTable.isDeltaTable(spark, Target_Path)):
      
      Target_Dimension_DF=spark.read.format("delta").load(Target_Path)
      max_id_value=max_id(Target_Dimension_DF,Dimension_ID,Join_Column)
  
      
      
      
      Dimension_Set_Insert = {Dimension_Col[k]: 'delta.'+Dimension_Col[k] for k in range(0, len(Dimension_Col))}
      Dimension_Set_Update=Dimension_Col_Update_Set(Dimension_Col,Join_Column,Dimension_ID)
      
     
      windowSpec  = Window.orderBy(Dimension_ID)
      
      
      Join_Data=Target_Dimension_DF.join(Source_DF,on=Join_Column,how='right')\
      .withColumn(Dimension_ID,col(Dimension_ID))\
      .select(Source_DF['*'],Dimension_ID)
      
      With_Null=Join_Data.filter(col(Dimension_ID).isNull())
      Without_Null=Join_Data.filter(col(Dimension_ID).isNotNull())
      Add_ID=With_Null.withColumn(Dimension_ID,max_id_value+row_number().over(windowSpec))
      Delta_Dimension_DF=Without_Null.union(Add_ID)
      
      
      
      
      print('Upsert logic Started running')
      Target_DF=merge_fun(Target_Path,Delta_Dimension_DF,Dimension_Set_Update,Dimension_Set_Insert,Join_Column)
      
      
    else:
      
      Delta_Dimension_DF=genrate_rowid(Source_DF,Dimension_ID,Join_Column[0])
      Delta_Dimension_DF.write.format("delta").mode("overwrite").save(Target_Path)
      
      Dim_Table_Create_Query = "create table business_temp.{0} using delta location '{1}'".format(Table_Names[i],Target_Path)
      print('Query to be executed',Dim_Table_Create_Query)
      spark.sql(Dim_Table_Create_Query)
      print('Delta Path Doesnt exist, Dimension table is created for:',Table_Names[i], 'with path: ',Target_Path )
dimension_load()


