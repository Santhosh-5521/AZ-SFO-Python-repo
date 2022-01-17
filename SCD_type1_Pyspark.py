
employee_src
employee_delta

employees_df = spark.sql(""" select * from employee_src """)
employees_delta_df = spark.sql(""" select * from employee_delta """)

emp_updated = employees_df.join(employees_delta_df,employees_df.emp_id == employees_delta_df.emp_id,how='inner')\
            .select(employees_delta_df.emp_id,employees_delta_df.emp_name,employees_delta_df.emp_city,employees_delta_df.emp_salary)
emp_updated.show();

emp_nochange_df = employees_df.join(employees_delta_df,employees_df.emp_id == employees_delta_df.emp_id,how='leftouter')\
                  .filter(employees_delta_df.emp_id.isNull())\
                  .select(employees_df.emp_id,employees_df.emp_name,employees_df.emp_city,employees_df.emp_salary)
emp_nochange_df.show()

emp_new_df = employees_df.join(employees_delta_df, employees_df.emp_id == employees_delta_df.emp_id, how = 'rightouter')\
             .filter(employees_df.emp_id.isNull())\
             .select(employees_delta_df.emp_id,employees_delta_df.emp_name,employees_delta_df.emp_city,employees_delta_df.emp_salary)
emp_new_df.show()


emp_final = emp_updated.unionAll(emp_nochange_df).unionAll(emp_new_df).orderBy('emp_id')
emp_final.show()

or

from functools import reduce
from pyspark.sql import DataFrame
 
def unionall(*df):
  return reduce(DataFrame.unionAll, df)
         
emp_final_1 = unionall(emp_updated, emp_nochange_df, emp_new_df).orderBy('emp_id')
emp_final_1.show()