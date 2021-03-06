package gobblin.applift.simpleconsumer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;

import gobblin.configuration.WorkUnitState;
import gobblin.source.extractor.extract.kafka.KafkaExtractor;
import kafka.message.MessageAndOffset;

public class KafkaSimpleLogExtractor extends KafkaExtractor<String, String>{

	public KafkaSimpleLogExtractor(WorkUnitState state) {
		super(state);
	}

	@Override
	public String getSchema() throws IOException {
		return this.topicName;
	}

	@Override
	protected String decodeRecord(MessageAndOffset messageAndOffset) throws IOException {
		return getString(messageAndOffset.message().payload());
	}
	
	protected static String getString(ByteBuffer buf) {
    byte[] bytes = new byte[0];
    if (buf != null) {
      int size = buf.remaining();
      bytes = new byte[size];
      buf.get(bytes, buf.position(), size);
    }
    String record = new String(bytes,Charset.forName("UTF-8"));
    return record;
  }
}
