
employee_src
employee_delta

employees_df = spark.sql(""" select * from employee_src""")
employees_delta_df = spark.sql(""" select * from employee_delta""")

from pyspark.sql import functions as F

##Inactive records

emp_inactive = employees_df.join(employees_delta_df,employees_df.emp_id == employees_delta_df.emp_id,how = 'inner')\
               .select(employees_df.emp_id,employees_df.emp_name,employees_df.emp_city,employees_df.emp_salary)\
               .withColumn('is_actv',F.lit('False'))\
               .withColumn('start_ts',F.current_timestamp())\
               .withColumn('end_ts',F.current_timestamp())
			   
##Active records

emp_active = employees_df.join(employees_delta_df,employees_df.emp_id == employees_delta_df.emp_id,how = 'inner')\
             .select(employees_delta_df.emp_id,employees_delta_df.emp_name,employees_delta_df.emp_city,employees_delta_df.emp_salary)\
             .withColumn('is_actv',F.lit('True'))\
             .withColumn('start_ts',F.current_timestamp())\
             .withColumn('end_ts',F.to_timestamp(F.lit('9999-12-31 00:00:00'),'yyyy-MM-dd HH:mm:ss'))
			 
##No change records
emp_nochange_df = employees_df.join(employees_delta_df,employees_df.emp_id == employees_delta_df.emp_id, how = 'leftouter')\
                  .select(employees_df.emp_id,employees_df.emp_name,employees_df.emp_city,employees_df.emp_salary)\
                  .filter(employees_delta_df.emp_id.isNull())\
                  .withColumn('is_actv',F.lit('True'))\
                  .withColumn('start_ts',F.current_timestamp())\
                  .withColumn('end_ts',F.to_timestamp(F.lit('9999-12-31 00:00:00'),'yyyy-MM-dd HH:mm:ss'))
				  
##Insert records
emp_new_df = employees_df.join(employees_delta_df,employees_df.emp_id == employees_delta_df.emp_id, how = 'rightouter')\
             .select(employees_delta_df.emp_id,employees_delta_df.emp_name,employees_delta_df.emp_city,employees_delta_df.emp_salary)\
             .filter(employees_df.emp_id.isNull())\
             .withColumn('is_actv',F.lit('True'))\
             .withColumn('start_ts',F.current_timestamp())\
             .withColumn('end_ts',F.to_timestamp(F.lit('9999-12-31 00:00:00'),'yyyy-MM-dd HH:mm:ss'))
			 
###Final PASS

from functools import reduce
from pyspark.sql import DataFrame

def unionAll(*df):
  return reduce(DataFrame.unionAll,df)

emp_final = unionAll(emp_inactive,emp_active,emp_nochange_df,emp_new_df).orderBy('emp_id','is_actv')

------------------------------------------------------------------------------------------------------------------------------------

emp_inactive_temp = employees_df.join(employees_delta_df,employees_df.emp_id == employees_delta_df.emp_id,how = 'inner')\
               .select(employees_df.emp_id,employees_df.emp_name,employees_df.emp_city,employees_df.emp_salary,employees_df.start_ts)\
               .filter(employees_df.is_actv == 'True')\
               .withColumn('is_actv',F.lit('False'))\
               .withColumn('end_ts',F.current_timestamp())
emp_inactive_temp.show()

emp_inactive = emp_inactive_temp.select('emp_id','emp_name','emp_city','emp_salary','is_actv','start_ts','end_ts')
emp_inactive.show()