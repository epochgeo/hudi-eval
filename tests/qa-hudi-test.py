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
conf.set('spark.driver.memory', '8G')

# Create SparkSession
spark = SparkSession.builder.master('local[10]')\
    .appName('qq-hudi-test')\
    .config(conf=conf)\
    .getOrCreate()

SedonaRegistrator.registerAll(spark)
spark.sparkContext.setLogLevel('WARN')

# QQ data with different schemas
# path = '/tank/tmp/blw-testing/hudi-tables/qa-hudi-test/raw-data/qa-no-lat-lon.snappy.parquet'
# path = '/tank/tmp/blw-testing/hudi-tables/qa-hudi-test/raw-data/qa-no-sfc-idx.snappy.parquet' 
path = '/tank/tmp/blw-testing/hudi-tables/qa-hudi-test/raw-data/qa-no-wifi.snappy.parquet'

df = spark.read.parquet(path)

# Hudi requies a UUID
df = df.withColumn('uuid', F.monotonically_increasing_id())

if 'horizontal_accuracy' in list(df.columns):
    print('Col `horizontal_accuracy` found, casting to double...')
    df = df.withColumn('horizontal_accuracy', df.horizontal_accuracy.cast(T.DoubleType()))

tableName = 'qatest'
schemaName = 'huditest'
partitionCol = 'sfc_part'

df = df.withColumn(partitionCol, F.lit('a'))

hudi_options = {
    'hoodie.database.name': schemaName,
    'hoodie.table.name': tableName,
    'hoodie.index.type': 'BLOOM',
    'hoodie.bloom.index.use.metadata': 'true',
    'hoodie.bloom.index.update.partition.path': 'true',
    'hoodie.metadata.index.bloom.filter.enable': 'true',
    'hoodie.metadata.index.bloom.filter.column.list': 'advertiser_id,wifi_bssid',
    'hoodie.schema.on.read.enable': 'true',
    'hoodie.parquet.compression.codec': 'gzip',
    'hoodie.parquet.small.file.limit': 104857600,
    'hoodie.parquet.max.file.size': 524288000,
    # Do not cluster on ingest, we will
    # Probably do this asynchronously
    'hoodie.clustering.async.enabled': 'false',
    'hoodie.clustering.inline': 'false',
    'hoodie.clustering.schedule.inline': 'false',
    'hoodie.clustering.plan.strategy.small.file.limit': 104857600, # Ignored
    'hoodie.clustering.plan.strategy.target.file.max.bytes': 524288000, # Ignored
    'hoodie.datasource.hive_sync.partition_fields': partitionCol,
    'hoodie.datasource.hive_sync.mode': 'hms',
    'hoodie.datasource.hive_sync.use_jdbc': 'false',
    'hoodie.datasource.hive_sync.metastore.uris': 'thrift://localhost:30983',
    'hoodie.datasource.hive_sync.auto_create_database': 'true',
    'hoodie.datasource.hive_sync.database': schemaName,
    'hoodie.datasource.hive_sync.table': tableName,
    'hoodie.datasource.hive_sync.base_file_format': 'PARQUET',
    'hoodie.datasource.hive_sync.enable': 'true',
    'hoodie.datasource.write.recordkey.field': 'uuid',
    'hoodie.datasource.write.partitionpath.field': partitionCol,
    'hoodie.datasource.write.table.name': tableName,
    'hoodie.datasource.write.table.type': 'MERGE_ON_READ',
    'hoodie.datasource.write.operation': 'insert',
    'hoodie.datasource.write.hive_style_partitioning': 'true',
    # These three are critical for supporting dynamic schemas
    'hoodie.datasource.write.schema.allow.auto.evolution.column.drop': 'true',
    'hoodie.datasource.write.reconcile.schema': 'true',
    'hoodie.schema.on.read.enable': 'true'
}

df.write.format('hudi')\
  .options(**hudi_options)\
  .mode('append')\
  .save('/tank/tmp/blw-testing/hudi-tables/qa-hudi-test/table')