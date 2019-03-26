package com.powsybl.server.storage;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.utils.KafkaTestUtils;

import com.powsybl.afs.storage.DefaultListenableAppStorage;
import com.powsybl.afs.storage.NodeInfo;
import com.powsybl.afs.storage.events.NodeEvent;

import org.apache.kafka.common.serialization.StringSerializer;

import org.springframework.kafka.support.serializer.JsonSerializer;

@TestConfiguration
@ComponentScan(basePackageClasses = {NodeEvent.class, NodeInfo.class, KafkaEmbeddedConfigTest.class, DefaultListenableAppStorage.class})
@Profile("test")
public class KafkaEmbeddedConfigTest {
    @Autowired
    private EmbeddedKafkaBroker embeddedKafka;

    @SuppressWarnings("unchecked")
    public ProducerFactory<Object, Object> producerFactory() {
        return new DefaultKafkaProducerFactory<>(KafkaTestUtils.producerProps(embeddedKafka), new StringSerializer(), new JsonSerializer());
    }
    @Bean
    public KafkaTemplate<Object, Object> getKafkaTemplate() {
        KafkaTemplate<Object, Object> kafkaTemplate = new KafkaTemplate<>(producerFactory());
        return kafkaTemplate;
    }
    @Bean
    public MessageListenerTest messageListener() {
        return new MessageListenerTest();
    }
}
