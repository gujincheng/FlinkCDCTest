/*
package com.digiwin.flink.cdc2kafka;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.nio.charset.Charset;

public class MyKafkaDeserializationSchema implements KafkaDeserializationSchema<String> {
    public static final Charset UTF_8 = Charset.forName("UTF-8");
    @Override
    public boolean isEndOfStream(String nextElement) {
        return false;
    }

    @Override
    public String deserialize(ConsumerRecord<byte[], byte[]> consumerRecord) throws Exception {
        String value = new String(consumerRecord.value(), UTF_8.name());
        long offset = consumerRecord.offset();
        int partition = consumerRecord.partition();
        return String.format("%s,%s,%s",value,offset,partition);
    }

    @Override
    public TypeInformation<String> getProducedType() {
        return TypeInformation.of(new TypeHint<String>(){});
        //return null; //会报错
    }
}
*/
