package kafka

import org.apache.kafka.clients.consumer.ConsumerConfig.*
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.StringDeserializer
import org.slf4j.LoggerFactory
import util.KAFKA_SERVER_LOCALHOST
import util.KOTLIN_SIMPLE_TOPIC
import java.time.Duration
import java.util.*

fun main() {
    val log = LoggerFactory.getLogger("SimpleConsumer")
    val props = Properties()
    props[BOOTSTRAP_SERVERS_CONFIG] = KAFKA_SERVER_LOCALHOST
    props[KEY_DESERIALIZER_CLASS_CONFIG] = StringDeserializer::class.java
    props[VALUE_DESERIALIZER_CLASS_CONFIG] = StringDeserializer::class.java
    props[GROUP_ID_CONFIG] = "group_01"

    val consumer = KafkaConsumer<String, String>(props)
    consumer.subscribe(listOf(KOTLIN_SIMPLE_TOPIC))
    while (true) {
    val records = consumer.poll(Duration.ofMillis(1000))
        records.iterator().forEach { record ->
            log.info("record key: ${record.key()}, record value: ${record.value()}, partition: ${record.partition()}")
        }
    }
}
