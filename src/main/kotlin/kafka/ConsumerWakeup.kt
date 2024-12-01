package kafka

import org.apache.kafka.clients.consumer.ConsumerConfig.*
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.errors.WakeupException
import org.apache.kafka.common.serialization.StringDeserializer
import org.slf4j.LoggerFactory
import util.KAFKA_SERVER_LOCALHOST
import util.KOTLIN_SIMPLE_TOPIC
import java.time.Duration
import java.util.*

fun main() {
    val log = LoggerFactory.getLogger("kafka-consumer")
    val props = Properties()
    props[BOOTSTRAP_SERVERS_CONFIG] = KAFKA_SERVER_LOCALHOST
    props[KEY_DESERIALIZER_CLASS_CONFIG] = StringDeserializer::class.java
    props[VALUE_DESERIALIZER_CLASS_CONFIG] = StringDeserializer::class.java
    props[GROUP_ID_CONFIG] = "group_01"
    props[AUTO_OFFSET_RESET_CONFIG] = "earliest"

    val consumer = KafkaConsumer<String, String>(props)
    consumer.subscribe(listOf(KOTLIN_SIMPLE_TOPIC))
    val mainThread = Thread.currentThread()
    Runtime.getRuntime().addShutdownHook(Thread {
        log.info("main process shutting down")
        consumer.wakeup()
        mainThread.join()
    })
    try {

    while (!Thread.interrupted()) {
        val records = consumer.poll(Duration.ofSeconds(1000))
        records.forEach {record ->
            log.info("recode key: ${record.key()}, partition: ${record.partition()}, offset: ${record.offset()}, record value: ${record.value()}")
        }
    }
    }catch (e: WakeupException) {
        log.error("wakeup exception has been called", e)
    }finally {
        log.info("finally consumer has been closed")
        consumer.close()
    }
}