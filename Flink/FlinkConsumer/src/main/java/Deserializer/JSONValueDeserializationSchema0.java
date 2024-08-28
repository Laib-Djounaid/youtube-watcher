package Deserializer;

import java.io.IOException;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import com.fasterxml.jackson.databind.ObjectMapper;

import Dto.YoutubeChannel;

public class JSONValueDeserializationSchema0 implements DeserializationSchema<YoutubeChannel> {

    public final ObjectMapper objectMapper = new ObjectMapper() ;
    @Override
    public void open(InitializationContext context) throws Exception {
        DeserializationSchema.super.open(context);
    }

    @Override
    public YoutubeChannel deserialize(byte[] bytes) throws IOException {
        return objectMapper.readValue(bytes, YoutubeChannel.class);
    }


    @Override
    public boolean isEndOfStream(YoutubeChannel youtubeChannel) {
        return false;
    }

    @Override
    public TypeInformation<YoutubeChannel> getProducedType() {
        return TypeInformation.of(YoutubeChannel.class);
    }

}
