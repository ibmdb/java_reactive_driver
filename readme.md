# IBM Db2 Java Reactive Driver

This Db2 database driver provides support for Java Reactive Programming using Spring Reactor. It implements R2DBC API.

## License
1. You will require Db2 Connect license or JCC license to use this product.
1. Licensing terms can be found in this [link](https://www.ibm.com/terms/?id=L-TGGQ-CSZLVM). Refer to license folder in the db2-r2dbc-1.1.0.jar file for Notices and License Information.

## Supported Features
1. Authentication - userid/password, SSL (server authentication), Kerberos.
1. Simple Connection and Connection Pooling.
1. SQL Statements – CREATE, SELECT, INSERT, UPDATE, DELETE, DROP.
1. Prepared Statement Caching.
1. Data Types – SMALLINT, INTEGER, BIGINT, DECIMAL/NUMERIC, DECFLOAT, REAL, DOUBLE, CHAR, VARCHAR, DATE, TIME, TIMESTAMP, BLOB, CLOB.
1. Support for Named Parameters.
1. Support for Data Transformations for parameter and result set data.

## Supported Platforms
1. DB2 on z/OS
1. DB2 on LUW

Note: We do not have support for iSeries yet.

## Setup
This driver is available through Maven Central Repository. Use the following dependency in your pom.xml.

```
    <dependency>
        <groupId>com.ibm.db2</groupId>
        <artifactId>db2-r2dbc</artifactId>
        <version>1.1.0</version>
    </dependency>
```

## Documentation
The following pdf documents are available in this repository. Please check the docs folder.
1. Installation Guide
1. Developer Guide
1. Debugging Guide

## API Documentation
Java API docs are available in the db2-r2dbc-1.1.0-javadoc.jar file. Consult your IDE instructions to add this javadoc jar to get context sensitive help. You can also download and extract this jar to get the html files containing the API doc.

## Samples
The following samples demonstrate how this driver can be used in a Java Reactive application. 
1. SampleApp.java
1. BlobSample.java
1. ClobSample.java

Please check the installation guide on how to setup and run these samples.

## Test Suite
This driver comes with unit tests that can be used to test the sanity of your environment. Please consult installation guide on how to setup and run these tests.

## Support
You can get support for this product through your regular IBM support channel for Db2 drivers. You can also use the issues section in this repository.

