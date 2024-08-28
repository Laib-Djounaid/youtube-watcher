package Deserializer;

import Dto.YoutubeVideo;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
// import org.apache.flink.util.Collector;

import java.io.IOException;

public class JSONValueDeserializationSchema implements DeserializationSchema<YoutubeVideo> {

    public final ObjectMapper objectMapper = new ObjectMapper() ;
    @Override
    public void open(InitializationContext context) throws Exception {
        DeserializationSchema.super.open(context);
    }

    @Override
    public YoutubeVideo deserialize(byte[] bytes) throws IOException {
        return objectMapper.readValue(bytes, YoutubeVideo.class);
    }


    @Override
    public boolean isEndOfStream(YoutubeVideo youtubeVideo) {
        return false;
    }

    @Override
    public TypeInformation<YoutubeVideo> getProducedType() {
        return TypeInformation.of(YoutubeVideo.class);
    }
}
