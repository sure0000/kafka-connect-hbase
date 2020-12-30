# 修改部分说明

修改位置一：
io.svectors.hbase.parser.JsonEventParser.toValue(java.util.Map<java.lang.String,java.lang.Object>, org.apache.kafka.connect.data.Field)
io.svectors.hbase.parser.JsonEventParser.toValue(java.lang.Object)

修改数据存储到hbase的类型，在 (issue 8)[https://github.com/nishutayal/kafka-connect-hbase/issues/8] 中有说明，Integer/Float/Long 类型默认在hbase中无法显示，
由于项目中的需要，将数据默认格式修改为String。

修改位置二：
io.svectors.hbase.parser.JsonEventParser.parse

由于公司项目中采用了 presto(Trino版本)，其中phoenix插件查询默认支持大写的 column，因此将 column 统一改成大写格式。


# Kafka Connect for Hbase

A Sink connector to write to HBase.  
This is enhanced version of implementation available at https://github.com/nishutayal/kafka-connect-hbase

## Features
Sink supports:
* Avro and JSON data format.
* Writing data to one or multiple column families in HBase Table
* Kafka payload field selection for each column family. In case of single column family, all columns are written to that column family. So no mapping definition is required.
* Row key selection
 
## Pre-requisites

* Kafka 1.0.0
* HBase 1.3.6
* JDK 1.8

## Assumptions
* The HBase table already exists.
* Column Families are already created in HBase table
* Each Kafka topic is mapped to a HBase table. (In version 1.0.1. implementation is changed- It requires `hbase.table.name` property to be set.)


## Properties

Below are the properties that need to be passed in the configuration file:

name | data type | required | description
-----|-----------|----------|------------
zookeeper.quorum | string | yes | Zookeeper quorum of the HBase cluster
event.parser.class | string | yes | Can be either AvroEventParser or JsonEventParser to parse avro or json events respectively.
topics | string | yes | list of kafka topics.
hbase.table.name | string | yes | name of hbase table
hbase.`<tablename>`.rowkey.columns | string | yes | The columns that represent the rowkey of the hbase table `<tablename>` 
hbase.`<tablename>`.family | string | yes | Comma seperated column families of the hbase table `<tablename>`. It can be one or more than one
hbase.`<tablename>`.`<columnfamily>`.columns | string | No | Required only if more than one column family are defined in previous configuration. Column names are comma seperated`. It can be one or more than one


Example connector.properties file

```bash
name=kafka-cdc-hbase
connector.class=io.svectors.hbase.sink.HBaseSinkConnector
tasks.max=1
topics=test
zookeeper.quorum=localhost:2181
event.parser.class=io.svectors.hbase.parser.AvroEventParser
hbase.table.name=destTable
hbase.destTable.rowkey.columns=id
hbase.destTable.rowkey.delimiter=|
hbase.destTable.family=c,d
hbase.destTable.c.columns=c1,c2
hbase.destTable.d.columns=d1,d2
```

## Packaging
* mvn clean package


## Deployment

* Follow the [Getting started](http://hbase.apache.org/book.html#standalone_dist) guide for HBase.

* [Download and install Confluent](http://www.confluent.io/)

* Add hbase-site.xml to hbase-sink.jar classpath 
```bash
			jar -uvf <hbase-sink.jar> hbase-site.xml
```

* Copy hbase-sink.jar and hbase-sink.properties from the project build location to `$CONFLUENT_HOME/share/java/kafka-connect-hbase`

```bash
mkdir $CONFLUENT_HOME/share/java/kafka-connect-hbase
cp target/hbase-sink.jar  $CONFLUENT_HOME/share/java/kafka-connect-hbase/
cp hbase-sink.properties $CONFLUENT_HOME/share/java/kafka-connect-hbase/
```

* Start Zookeeper, Kafka and Schema registry

```bash
nohup $CONFLUENT_HOME/bin/zookeeper-server-start $CONFLUENT_HOME/etc/kafka/zookeeper.properties &
nohup $CONFLUENT_HOME/bin/kafka-server-start $CONFLUENT_HOME/etc/kafka/server.properties &
nohup $CONFLUENT_HOME/bin/schema-registry-start $CONFLUENT_HOME/etc/schema-registry/schema-registry.properties &"
```

* Create HBase table 'destTable' from hbase shell

* Start the hbase sink

```bash
export CLASSPATH=$CONFLUENT_HOME/share/java/kafka-connect-hbase/hbase-sink.jar

$CONFLUENT_HOME/bin/connect-standalone etc/schema-registry/connect-avro-standalone.properties etc/kafka-connect-hbase/hbase-sink.properties
```

* Test with avro console, start the console to create the topic and write values

```bash
$CONFLUENT_HOME/bin/kafka-avro-console-producer \
--broker-list localhost:9092 --topic test \
--property value.schema='{"type":"record","name":"record","fields":[{"name":"id","type":"int"}, {"name":"name", "type": "string"}]}'
```

```bash
#insert at prompt
{"id": "1", "name": "foo"}
{"id": "2", "name": "bar"}
```

Same can be run with Apache Kafka scripts. 

`$KAFKA_HOME/bin/connect-standalone.sh config/connect-standalone.properties config/hbase-sink.properties`

* For JSON data, use JSONConverter in connect-standalone.properties file. If key and value contain schema and payload, set schema.enable to `true`, else `false`. 

Example data [with schema] : 
  
`
{"schema":{"type":"struct","fields":[{"type":"string","optional":false,"field":"firstName"},{"type":"string","optional":false,"field":"lastName"},{"type":"string","optional":false,"field":"email"},{"type":"int32","optional":false,"field":"age"},{"type":"int32","optional":false,"field":"weightInKgs"}],"optional":false,"name":"Person"},"payload":{"firstName":"Jan","lastName":"Peter","email":"eric.cartman@southpark.com","age":10,"weightInKgs":40}}
`

  [without schema]:
 
 `{"firstName":"Jan","lastName":"Peter","email":"eric.cartman@southpark.com","age":10,"weightInKgs":40}`
 

###Changes

#####v1.0.1
 - Earlier limitation "kafka topic and table name should be same" has been removed. Table name can be set using `hbase.table.name` property  
 - Upgraded to Kafka 2.0.0 and Confluent 5.0.0
 
#####v1.0.0
 - Initial version 
 - Support with Kafka 1.0.0 and Confluent 4.0.0 
 
 
