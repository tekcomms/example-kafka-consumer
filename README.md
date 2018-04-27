# Export Consumer Example - an example provided as a utility for the export pipeline

This project produces a shaded jar containing all dependencies directly.

### Unsecured Kafka Cluster
To run in an unsecured Kafka cluster run the following

java -jar bda-export-consumer-example-0.2-SNAPSHOT.jar

Use --help to get a list of options

### Secured Kafka Cluster
To run in a secured Kafka cluster run the following with the -p option at a minimum.  The presence of this option enables security.  Example configuration files are provided.

For details on the krb5.conf file, see https://web.mit.edu/kerberos/krb5-1.12/doc/admin/conf_files/krb5_conf.html
For details on the *_jaas.conf file, see https://docs.oracle.com/javase/8/docs/jre/api/security/jaas/spec/com/sun/security/auth/module/Krb5LoginModule.html

java -Djava.security.krb5.conf=<fullpath/krb5.conf> -Djava.security.auth.login.config=<fullpath/kafka_client_jaas.conf> -jar bda-export-consumer-example-0.2-SNAPSHOT.jar -p <truststorepass>

Use --help to get a list of options

Note:
 Confluent Schema registry is used for Avro serialization. https://docs.confluent.io/current/app-development/index.html#java
 You may need to add the confluent repositories to your maven settings.xml to get the correct libraries.
 
 ```xml
 <repositories>
    <repository>
        <id>confluent</id>
        <url>http://packages.confluent.io/maven/</url>
    </repository>
 </respositories>
 ```
 
 