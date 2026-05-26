import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrameCollection
from awsgluedq.transforms import EvaluateDataQuality
from awsglue.dynamicframe import DynamicFrame
from awsglue import DynamicFrame

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
# Script generated for node Custom Transform
def MyTransform(glueContext, dfc) -> DynamicFrameCollection:
    from awsglue.dynamicframe import DynamicFrame, DynamicFrameCollection
    from pyspark.sql.functions import col, udf, when, lit
    from pyspark.sql.types import StringType
    import re
    from dateutil import parser

    # ------------------------------------------------------------
    # STEP 0: Extract first frame
    # ------------------------------------------------------------
    dyf = dfc.select(list(dfc.keys())[0])
    df = dyf.toDF()

    # ------------------------------------------------------------
    # STEP 1: Date parsing logic
    # ------------------------------------------------------------
    def extract_date(s):
        try:
            if s is None:
                return None

            s = str(s).strip()

            if s.lower() in ["nan", "null", "", "none", "na"]:
                return None

            if "GregorianCalendar" in s:
                y = re.search(r"YEAR=(\d+)", s)
                m = re.search(r"MONTH=(\d+)", s)
                d = re.search(r"DAY_OF_MONTH=(\d+)", s)

                if y and m and d:
                    return f"{y.group(1)}-{int(m.group(1))+1:02d}-{d.group(1)}"
                return None

            return parser.parse(s, fuzzy=True).date().isoformat()

        except:
            return None

    parse_udf = udf(extract_date, StringType())

    # ------------------------------------------------------------
    # STEP 2: Clean column
    # ------------------------------------------------------------
    df = df.withColumn(
        "reading_date",
        when(parse_udf(col("reading_date")).isNull(), lit("1999-01-01"))
        .otherwise(parse_udf(col("reading_date")))
    )

    df = df.select(*df.columns)

    # ------------------------------------------------------------
    # STEP 3: Convert back to DynamicFrame
    # ------------------------------------------------------------
    df_res = DynamicFrame.fromDF(df, glueContext, "df_res")

    # ------------------------------------------------------------
    # STEP 4: RETURN COLLECTION (REQUIRED BY GLUE)
    # ------------------------------------------------------------
    return DynamicFrameCollection(
        {"CustomTransform0": df_res},
        glueContext
    )
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# Script generated for node Amazon S3
AmazonS3_node1779732605151 = glueContext.create_dynamic_frame.from_catalog(database="bronze", table_name="bronze_distance_log", transformation_ctx="AmazonS3_node1779732605151")

# Script generated for node Change Schema
ChangeSchema_node1779670716361 = ApplyMapping.apply(frame=AmazonS3_node1779732605151, mappings=[("origin_1", "string", "origin_city", "string"), ("origin_2", "string", "origin_province", "string"), ("destination_1", "string", "destination_city", "string"), ("destination_2", "string", "destination_province", "string"), ("start_odometer", "long", "start_odometer", "bigint"), ("end_odometer", "long", "end_odometer", "bigint"), ("distance_km", "long", "distance_km", "bigint"), ("date", "string", "reading_date", "varchar"), ("partition_0", "string", "partition_0", "string")], transformation_ctx="ChangeSchema_node1779670716361")

# Script generated for node Custom Transform
CustomTransform_node1779680829999 = MyTransform(glueContext, DynamicFrameCollection({"ChangeSchema_node1779670716361": ChangeSchema_node1779670716361}, glueContext))

# Script generated for node Select From Collection
SelectFromCollection_node1779689410632 = SelectFromCollection.apply(dfc=CustomTransform_node1779680829999, key=list(CustomTransform_node1779680829999.keys())[0], transformation_ctx="SelectFromCollection_node1779689410632")

# Script generated for node Change Schema
ChangeSchema_node1779690484957 = ApplyMapping.apply(frame=SelectFromCollection_node1779689410632, mappings=[("origin_city", "string", "origin_city", "string"), ("origin_province", "string", "origin_province", "string"), ("destination_city", "string", "destination_city", "string"), ("destination_province", "string", "destination_province", "string"), ("start_odometer", "long", "start_odometer", "bigint"), ("end_odometer", "long", "end_odometer", "bigint"), ("distance_km", "long", "distance_km", "bigint"), ("reading_date", "string", "reading_date", "date")], transformation_ctx="ChangeSchema_node1779690484957")

# Script generated for node SQL Query
SqlQuery19 = '''
SELECT *
FROM distance_log
'''
SQLQuery_node1779671769309 = sparkSqlQuery(glueContext, query = SqlQuery19, mapping = {"distance_log":ChangeSchema_node1779690484957}, transformation_ctx = "SQLQuery_node1779671769309")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=SQLQuery_node1779671769309, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1779664358455", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1779673231530 = glueContext.write_dynamic_frame.from_options(frame=SQLQuery_node1779671769309, connection_type="s3", format="glueparquet", connection_options={"path": "s3://goa-data-pipeline-silver-ap-ca-central-1-lab/silver_distance_log/", "partitionKeys": []}, format_options={"compression": "snappy"}, transformation_ctx="AmazonS3_node1779673231530")

job.commit()