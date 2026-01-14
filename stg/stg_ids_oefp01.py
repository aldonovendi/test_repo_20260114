import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from datetime import datetime,timedelta
from pyspark.sql import functions as f
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.conf import SparkConf

<<<<<<< HEAD
abcde

=======
>>>>>>> parent of 7cf784c (test)
conf = SparkConf() \
    .set("spark.sql.sources.partitionOverwriteMode","dynamic") \
    .set("spark.sql.parquet.enableVectorizedReader", "false") \

args = getResolvedOptions(sys.argv, ['JOB_NAME', "TempDir", 'env'])

spark = SparkContext(conf = conf)
glueContext = GlueContext(spark)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args['JOB_NAME'], args)

file = 'oefp01'
table_name = f'ing_ids_{file}'
env = args['env']
destination = 's3://hmsi-{0}-data-bucket/stg/ids/'.format(env)
target_path = destination + f"stg_ids_{file}/"
database = "ing"

DataSource0 = glueContext.create_dynamic_frame.from_catalog(database = database, table_name = table_name, transformation_ctx = "DataSource0")
df0 = DataSource0.toDF()
row_count = df0.count()

if row_count > 0:
    int_cols = ['INMME1'
    ,'INDDE1'
    ,'DEMME1'
    ,'DEDDE1'
    ,'DSMME1'
    ,'DSDDE1'
    ,'ETMME1'
    ,'ETDDE1'
    ,'ODMME1'
    ,'ODDDE1'
    ,'CPMME1'
    ,'IPMME1'
    ,'INYYE1'
    ,'DEYYE1'
    ,'DSYYE1'
    ,'ETYYE1'
    ,'ODYYE1'
    ,'CPYYE1'
    ,'IPYYE1'
    ,'LINEE1'
    ,'OLINE1'
    ,'CLRSE1'
    ,'XLINE1'
    ,'GLINE1'
    ,'ILINE1'
    ,'RUN#E1'
    ,'PSEQE1'
    ,'LUPDE1'
    ,'DPTME1'
    ,'RLTME1'
    ,'REF#E1'
    ,'PCK#E1'
    ,'OOQE1'
    ,'SORDE1'
    ,'SUPQE1'
    ,'OR#E1'
    ,'XR#E1'
    ,'GR#E1'
    ,'DISQE1'
    ,'INV#E1'
    # ,'OCSQE1'
    ]
    for col_name in int_cols:
        df0 = df0.withColumn(col_name, f.col(col_name).cast("int"))
    long_cols = [
    'WSLEE1'
    ,'OPRCE1'
    ,'INVVE1'
    ,'TAXVE1'
    ,'TAXBE1'
    ,'TXCRE1']
    for col_name in long_cols:
        df0 = df0.withColumn(col_name, f.col(col_name).cast("long"))
        
    double_cols = ['DSCPE1','DISRE1','COSTE1','ICSTE1','UFN1E1','UFN2E1','UFN3E1']
    for col_name in double_cols:
        df0 = df0.withColumn(col_name, f.col(col_name).cast("double"))
    
    df0 = df0.withColumn('prc_dt', f.lit(datetime.now()))
    
    df0.coalesce(1).write.partitionBy(['ODYYE1','ODMME1']).mode('overwrite').format('parquet').save(target_path)
else:
    pass


job.commit()
