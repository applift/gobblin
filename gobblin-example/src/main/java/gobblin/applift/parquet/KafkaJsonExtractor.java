package gobblin.applift.parquet;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;

import org.apache.avro.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gobblin.configuration.WorkUnitState;
import gobblin.metrics.kafka.SchemaRegistryException;
import gobblin.source.extractor.extract.kafka.KafkaExtractor;
import kafka.message.MessageAndOffset;

/**
 * @author prashant.bhardwaj@applift.com
 *
 */
public class KafkaJsonExtractor extends KafkaExtractor<Schema, String> {

  private static final Logger log = LoggerFactory.getLogger(KafkaJsonExtractor.class);

  protected final KafkaAvroSchemaRegistry schemaRegistry;
  protected final Schema schema;

  public KafkaJsonExtractor(WorkUnitState state) {
    super(state);
    this.schemaRegistry = new KafkaAvroSchemaRegistry(state.getProperties());
    this.schema = getExtractorSchema();
  }

  /**
   * Get the schema to be used by this extractor.
   */
  protected Schema getExtractorSchema() {
    return getLatestSchemaByTopic();
  }

  private Schema getLatestSchemaByTopic() {
    try {
      return this.schemaRegistry.getLatestSchemaByTopic(this.topicName);
    } catch (SchemaRegistryException e) {
      log.error(String.format("Cannot find latest schema for topic %s. This topic will be skipped", this.topicName), e);
      return null;
    }
  }

  @Override
  protected String decodeRecord(MessageAndOffset messageAndOffset) throws IOException {
    return getJsonString(messageAndOffset.message().payload());
  }

  /**
   * Get the schema (metadata) of the extracted data records.
   *
   * @return the Schema of Kafka topic
   * @throws IOException
   *           if there is problem getting the schema
   */
  @Override
  public Schema getSchema() throws IOException {
    return this.schema;
  }

  /**
   * Returns Json String from payload byte buffer.
   * 
   * @param buf
   * @return Json String
   */

  protected static String getJsonString(ByteBuffer buf) {
    byte[] bytes = new byte[0];
    if (buf != null) {
      int size = buf.remaining();
      bytes = new byte[size];
      buf.get(bytes, buf.position(), size);
    }
    String jsonString = new String(bytes, Charset.forName("UTF-8"));
    return jsonString;
  }
}
