Spark Cassandra Demo With Zeppelin
====================

This branch has been built to highlight the integration of Spark and Cassandra.

Step to build Zeppelin for **DSE SearchAnalytics** demos:

1. A working **Cassandra** server, version **2.1.x** or **Datastax Enterprise 4.7.1** at least
2. `wget` and `curl` commands **installed** and **working**
3. A working **Maven 3.1.x** installed
4. A working **[Bower]** installed
5. Your **cqlsh** should be in the `$PATH` variable. Check with `which cqlsh` in a shell. If not found, add your `$CASSANDRA_HOME/bin` to `$PATH` 
6. Clone this branch : `git clone -b CassandraInterpreterWithSamples https://github.com/doanduyhai/incubator-zeppelin` locally
7. Copy the existing `$ZEPPELINE_HOME/conf/zeppelin-site.xml.template` to make your own `$ZEPPELINE_HOME/conf/zeppelin-site.xml` 
8. You need to tune the `$ZEPPELINE_HOME/conf/zeppelin-site.xml` file to change the default Zeppelin **port**. I set it to **9080** to avoid any conflict with the **Spark Master GUI** running on port **8081**
    
    ```xml
    <property>
        <name>zeppelin.server.port</name>
        <value>9080</value>
        <description>Server port. port+1 is used for web socket.</description>
    </property>
    ```
9. Build your own **Zeppelin** with the Spark-Cassandra connector embedded using the special profile `cassandra-spark-1.3`:
 `mvn clean package -Pcassandra-spark-1.3 -Dhadoop.version=2.2.0 -Phadoop-2.2 -DskipTests`
10. Start your **Cassandra** or **DSE 4.7.1** server in **Search** mode: `$DSE_HOME/bin/dse cassandra -s`
11. Start the **Zeppelin** server with `$ZEPPELIN_HOME\bin\zeppelin-daemon.sh start`
12. Open the browser at `http://localhost:9080/` to connect to **Zeppelin**
13. Click on the **Interpreter** menu, **Spark** section, ensure that property `master` is set to `local[*]` and add a new property `spark.cassandra.connection.host` and set its value to the address on which **Cassandra** is listening (usually `localhost` or `127.0.0.1`). **Do not forget to click on the + (plus) icon otherwise Zeppelin will not add your new property**

    <center>![Zeppelin Interpreter](https://raw.githubusercontent.com/doanduyhai/incubator-zeppelin/Spark_Cassandra_Demo/assets/Zeppelin_Interpreter.png "Zeppelin Interpreter Parameters")</center>
14. After adding this property and clicking on **Save**, **Zeppelin** will prompt you to accept restarting the interpreter, validate with **Yes**
15. The demo starts with the notebook **00-Prepare Cassandra Data For Demo**. This notebook will create and populate the data necessary for the demos 
16. The notebooks **07-DSE Search in action** and **08-SearchAnalytics Mode** require **Datastax Enterprise 4.7.1** at least. Do not run them if you're using an open-source **Cassandra**
17. The notebook **09-Demos Cleanup** will clean up all data downloaded and created for those demos

[Bower]: http://bower.io/

