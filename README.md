# SCATS Application Modules

Apache Spark applications for processing SCATS data using GeoMesa / Accumulo

### Prerequisities

 - Java 1.8 or above
 - Maven 3
 - Apache Hadoop Client (local instance for uploading jar)
 - Apache Spark 1.6.0 or above (local instance for spark-submit)

### Installing

Clone or download this project.

```
git clone https://github.com/project-rhd/scats-app
```

### Running Unit Tests

Launch the unit test via maven. The unit tests includ running this application in Spark's local mode

```
cd ./scats-app
mvn test
```

### Usage

1) Create a launch script via provided tamplate under ./bin directory.

```
cd ./scats-app/bin
cp importScats.sh.tamp importScats.sh && cp aggreDOW.sh.tamp aggreDOW.sh
chmod +x *.sh
```

2) Configure the "Primary Settings" inside *.sh (including IP of spark master, input file path, etc...). Examples:

```
## Ip address of spark master.
spark_master_address=
## Ip address of hadoop master. (the name node)
hadoop_master_address=
## Name of the jar to be uploaded. e.g. SCATS-RawDataCleaner-0.1.0.jar
jar_name=SCATS-DataImporter-0.2.0.jar
## Directory in hdfs for placing jar. e.g. /scats
jar_hdfs_dir=/scats

## Local Spark home directory e.g. /usr/local/spark
client_spark_home=
## Local Hadoop home e.g. /usrl/local/hadoop
client_hadoop_home=

## Accumulo Parameters
accumulo_instance_id=
zookeepers=scats-1-master:2181,scats-1-slave:2181
accumulo_user_name=
accumulo_user_pwd=
accumulo_table_name=
accumulo_geoFeature_name=

## Path of raw data file in hdfs. e.g. /scats/sample700M.csv  VolumeData.CSV  VolumeData.sample.csv
INPUT_VOLUME_FILE=/scats/VolumeData.sample.csv
INPUT_LAYOUT_FILE=/scats/SiteLayouts.csv
INPUT_SHAPE_FILE=/scats/scats_site_vic_4326.csv

```

3) Execute scripts under ./bin
- Script for importing scats raw data into GeoMesa / Accumulo
```
./importScats.sh
```
- Script for aggregating scats data and stored results into a new feature schema
```
./aggreDOW.sh
```
### Work flow of bin scripts
1. Build application jar locally after unit tests
2. Upload packaged jar to the specified HDFS directory
3. Submit the task description to remote Spark master via spark-submit


