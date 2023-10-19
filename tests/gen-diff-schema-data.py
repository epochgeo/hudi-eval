from pyspark import SparkConf
from pyspark import StorageLevel
from pyspark.sql import SparkSession
from pyspark.sql import functions as F, types as T
from sedona.register import SedonaRegistrator
from sedona.utils import SedonaKryoRegistrator, KryoSerializer

conf = SparkConf()
conf.set('spark.sql.session.timeZone', 'UTC')
conf.set('mapreduce.fileoutputcommitter.marksuccessfuljobs', 'true')
conf.set('spark.hadoop.fs.s3a.aws.credentials.provider', 'com.amazonaws.auth.DefaultAWSCredentialsProviderChain')
conf.set('spark.hadoop.fs.s3a.impl', 'org.apache.hadoop.fs.s3a.S3AFileSystem')
conf.set('spark.serializer', KryoSerializer.getName)
conf.set('spark.kryo.registrator', SedonaKryoRegistrator.getName)
conf.set('spark.sql.debug.maxToStringFields', 100)
conf.set('spark.dynamicAllocation.enabled', 'false')
conf.set('spark.default.parallelism', 80)
conf.set('spark.eventLog.enabled', 'true')
conf.set('spark.kryoserializer.buffer.max', '2000m')
conf.set('spark.sql.catalog.spark_catalog', 'org.apache.spark.sql.hudi.catalog.HoodieCatalog')
conf.set('spark.sql.extensions', 'org.apache.spark.sql.hudi.HoodieSparkSessionExtension')

# Create SparkSession
spark = SparkSession.builder.master('local[10]')\
    .appName('hudi-hello-world')\
    .config(conf=conf)\
    .getOrCreate()

SedonaRegistrator.registerAll(spark)
spark.sparkContext.setLogLevel('WARN')

qa1 = '/tank/ege/qa/daily_v3/location/2023/06/13/part-00245-39fedf43-a98b-47d5-9fd2-08a299ac07df-c000.gz.parquet'
qa2 = '/tank/ege/qa/daily_v3/location/2023/06/13/part-00488-39fedf43-a98b-47d5-9fd2-08a299ac07df-c000.gz.parquet'
qa3 = '/tank/ege/qa/daily_v3/location/2023/06/13/part-00499-39fedf43-a98b-47d5-9fd2-08a299ac07df-c000.gz.parquet'

qa1_no_lat_lon = spark.read.parquet(qa1).drop('latitude', 'longitude').coalesce(1)
qa2_no_sfc_idx = spark.read.parquet(qa2).drop('sfc_index').coalesce(1)
qa3_no_wifi = spark.read.parquet(qa3).drop('wifi_ssid', 'wifi_bssid').coalesce(1)

qa3_no_wifi.write.format('parquet')\
  .option('compression', 'snappy')\
  .mode('append')\
  .save('/tank/tmp/blw-testing/hudi-tables/qa-hudi-test/data')