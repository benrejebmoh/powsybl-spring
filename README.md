# powsybl-spring


Dans les fichiers cloud-config : listener.yml 

# Listener : adresse du consumer
kafka:
  bootstrapAddress: localhost:9092

# Adresse du broker
spring:
  kafka:
    bootstrap-servers: localhost:9092

<pre>
Dans le module listener bootstrap.yml

spring:
  application:
    name: listener
  cloud:
    config:
      uri: http://\<ip_config\>:\<port_config\>
</pre>

    
# Kafka

- Télécharger : http://kafka.apache.org/
- Décompression : dans $KAFKA_HOME

# Lancement : minimale

- $KAFKA_HOME/bin/zookeeper-server-start.sh config/zookeeper.properties
- $KAFKA_HOME/bin/kafka-server-start.sh config/server.properties

