Spark Cassandra Demo
====================

This branch has been built to highlight the integration of Spark and Cassandra.

For the demos to work, you'll need:

1. A working **Cassandra** server, version **2.0.x**, **2.1.x** or **Datastax Enterprise 4.7.x**
2. A running **Spark** cluster deployed in **stand-alone mode** (a stand-alone local deployment is fine)
3. You may need to tune the `$ZEPPELINE_HOME/conf/zeppelin-env.sh` file to change the **MASTER** parameter. By default it is set to `spark://127.0.0.1:7077`
4. You may need to tune the `$ZEPPELINE_HOME/conf/zeppelin-site.xml` file to change the default Zeppelin **port**. I set it to **9080** to avoid any conflict with the **Spark Master GUI** running on port **8081**
    
    ```xml
    
    <property>
        <name>zeppelin.server.port</name>
        <value>9080</value>
        <description>Server port. port+1 is used for web socket.</description>
    </property>
    ```
    
5. You **should** add the parameter `spark.cassandra.connection.hosts` to the **Spark Interpreter** config. I set the value to `127.0.0.1` for Cassandra host name but you can adapt it to fit your own Cassandra deployment.
    <center>![Zeppelin Interpreter](https://raw.githubusercontent.com/doanduyhai/incubator-zeppelin/Spark_Cassandra_Demo/assets/Zeppelin_Interpreter.png "Zeppelin Interpreter Parameters")</center>
6. The notebooks **07-DSE Search in action** and **08-SearchAnalytics Mode** require **Datastax Enterprise 4.7.x**. Do not run them if you're using an open-source **Cassandra**
7. The notebook **09-Demos Cleanup** will clean up all data downloaded and created for those demos
8. `wget` and `curl` commands **installed** and **working**
9. A working **Maven 3.x** installed

## Running instructions 

1. Clone my Zeppelin forked repo (until my pull requests are merged into official master): `git clone -b Spark_Cassandra_Demo https://github.com/doanduyhai/incubator-zeppelin`
2. Build your own **Zeppelin** with the Spark-Cassandra connector embedded using the special profile `cassandra-spark-1.2`:
    `mvn clean package -Pcassandra-spark-1.2 -Dhadoop.version=2.5.0-cdh5.3.0 -Phadoop-2.4 -DskipTests`
3. Start your **Cassandra** server
4. Start the **Zeppelin** server with `$ZEPPELIN_HOME\bin\zeppelin-daemon.sh start`
5. Open the browser at `http://localhost:9080/` and choose the notebook **00-Prepare Cassandra Data For Demo**

For the original **Zeppelin** README, click [here]

[here]: https://github.com/apache/incubator-zeppelin/blob/master/README.md
