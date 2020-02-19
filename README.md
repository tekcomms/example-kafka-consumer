# Export Consumer Example - an example provided as a utility for the export pipeline

This project produces a shaded jar containing all dependencies directly.

### Unsecured Kafka Cluster
To run in an unsecured(PLAINTEXT) Kafka Avro cluster run the following
java -jar bda-export-consumer-example-0.4-SNAPSHOT.jar -c <client_id> -k <kafka_host>:<kafka_port> -b <records_base> -t <topics> -m <max_records_per_file> -d avro -r <schema_registry_url>

Remove -d and -r option to run in an unsecured(PLAINTEXT) Kafka Json cluster.
java -jar bda-export-consumer-example-0.4-SNAPSHOT.jar -c <client_id> -k <kafka_host>:<kafka_port> -b <records_base> -t <topics> -m <max_records_per_file>

Use --help to get a list of options
java -jar bda-export-consumer-example-0.4-SNAPSHOT.jar -h

### Secured Kafka Cluster
To run in a secured Kafka cluster run the following with the -sp option<SSL/SASL_PLAINTEXT/SASL_SSL> at a minimum.  The presence of this option enables security.  Example configuration files are provided.

For details on the krb5.conf file, see https://web.mit.edu/kerberos/krb5-1.12/doc/admin/conf_files/krb5_conf.html
For details on the *_jaas.conf file, see https://docs.oracle.com/javase/8/docs/jre/api/security/jaas/spec/com/sun/security/auth/module/Krb5LoginModule.html

To run kafka Avro with sasl/gssapi + ssl/two_way_auth (SASL_SSL)
java -Djava.security.krb5.conf=<fullpath/krb5.conf> -Djava.security.auth.login.config=<fullpath/kafka_client_jaas.conf> -jar bda-export-consumer-example-0.1-SNAPSHOT.jar -Dkafka_sasl_mechanism=GSSAPI -d=avro -sp SASL_SSL tf <fullpath_truststore_cert> -tp <truststore_pass> -kf <fullpath_keystore_cert> -kp <keystore_pass> -r <schema_registry_url> 

Remove -d and -r option to run with Kafka Json.
Remove -Djava.security.krb5.conf option and update -Dkafka_sasl_mechanism type if using another SASL mechanism option as <PLAIN/SCRAM-SHA-512/SCRAM-SHA-256>.
Remove -Djava.security.krb5.conf, -Djava.security.auth.login.config and -Dkafka_sasl_mechanism options if not use SASL security option.
Remove -kf and -kp options if SSL does not require two way authentication.
Remove tf, -tp, -kf and -kp options if does not using SSL security (SASL_PLAINTEXT)

###Extra kafka option if it requires
Use --help or -h to get a list of options
java -jar bda-export-consumer-example-0.4-SNAPSHOT.jar -h

### Unsecured Pulsar Cluster
java -jar bda-export-consumer-example-0.4-SNAPSHOT.jar -p true

### Secured Pulsar Cluster
To run in TLS enable only
java -jar bda-export-consumer-example-0.4-SNAPSHOT.jar -p true -pe true -pt <fullpath_truststore_file>

To run in TLS authentication
java -jar bda-export-consumer-example-0.4-SNAPSHOT.jar -p true -pe true -pt <fullpath_truststore_file> -pa true -pc <fullpath_cert_file> -pk <fullpath_keystore_file>

For detail on TLS enable only https://pulsar.apache.org/docs/en/security-tls-transport/
For detail on TLS authentication https://pulsar.apache.org/docs/en/security-tls-authentication/

java -Djava.security.krb5.conf=<fullpath/krb5.conf> -Djava.security.auth.login.config=<fullpath/kafka_client_jaas.conf> -jar bda-export-consumer-example-0.2-SNAPSHOT.jar -p <truststorepass>

Use --help to get a list of options

### Confluent Schema Registry
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