package io.woolford;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.maxmind.geoip2.DatabaseReader;
import com.maxmind.geoip2.record.Location;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.KStream;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.util.Map;
import java.util.Properties;

public class SrcDstGeoEnricher {

    private static File database = new File("src/main/resources/GeoIP2-City.mmdb");
    private static DatabaseReader reader;

    static {
        try {
            reader = new DatabaseReader.Builder(database).build();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static JsonObject getLocationFromIp(String ip) {

        Location location;
        try {
            location = reader.city(InetAddress.getByName(ip)).getLocation();
        } catch (Exception e) {
            location = null;
        }

        JsonObject jsonLocation = new JsonObject();
        jsonLocation.addProperty("ip", ip);

        if (location != null){
            JsonObject latLon = new JsonObject();
            latLon.addProperty("lat", location.getLatitude());
            latLon.addProperty("lon", location.getLongitude());
            jsonLocation.add("location", latLon);
        }

        return jsonLocation;
    }

    private static String enrichSrcDest(String srcDestMessage) throws IOException {

        Gson gson = new Gson();
        Map srcDstMessageMap = gson.fromJson(srcDestMessage, Map.class);

        String src = srcDstMessageMap.get("src").toString();
        String dst = srcDstMessageMap.get("dst").toString();

        JsonObject srcJsonObject;
        try {
            srcJsonObject = getLocationFromIp(src);
            srcDstMessageMap.put("src", srcJsonObject);
        } catch (Exception e) {
            // ignore
        }

        JsonObject dstJsonObject;
        try {
            dstJsonObject = getLocationFromIp(dst);
            srcDstMessageMap.put("dst", dstJsonObject);
        } catch (Exception e) {
            // ignore
        }

        return gson.toJson(srcDstMessageMap);

    }

    public static void main(String[] args) {

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, KafkaConstants.APPLICATION_ID);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConstants.KAFKA_BROKERS);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        final StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> srcDst = builder.stream(KafkaConstants.ORIGIN_TOPIC_NAME);

        srcDst.map((k, v) -> {
            try {
                return new KeyValue<>(k, enrichSrcDest(v));
            } catch (IOException e) {
                return null;
            }
        }).to(KafkaConstants.DESTINATION_TOPIC_NAME);

        final Topology topology = builder.build();

        final KafkaStreams streams = new KafkaStreams(topology, props);
        streams.start();

    }

}

