import os
from datetime import datetime

from alfalib.alfa_spark.spark_class import AlfaSpark

from pipelines.handler import Handler


# We retrieve the start date of the airflow dag from the environment variable and convert it to the datetime type (if necessary).
ds = os.environ['ds']
ds = datetime.strptime(ds, '%Y-%m-%d')

# Parameters for starting the Spark session, it is necessary to specify spark_app_name.
spark_app_name = ""
spark_user = "user"
spark_app_config = {"deploy-mode": "client",
                    "spark.executor.instances": 4,
                    "spark.executor.memory": "4g",
                    "spark.executor.cores": 2,
                    "spark.driver.memory": "2g"
                    }

spark_class = AlfaSpark(spark_user, spark_app_name, spark_app_config)
spark = spark_class.get_spark_session()

# Code for further data processing through the custom Handler class.
handler = Handler(spark)
