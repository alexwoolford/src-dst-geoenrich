package io.woolford;

public interface KafkaConstants {

    public static String KAFKA_BROKERS = "cp01.woolford.io:9092,cp02.woolford.io:9092,cp03.woolford.io:9092";
    public static String APPLICATION_ID = "src-dst-geoenrich";
    public static String ORIGIN_TOPIC_NAME = "src-dst";
    public static String DESTINATION_TOPIC_NAME = "src-dst-enriched";

}