package gobblin.applift.parquet;

import java.io.IOException;

import org.apache.avro.Schema;

import gobblin.configuration.WorkUnitState;
import gobblin.source.extractor.Extractor;
import gobblin.source.extractor.extract.kafka.KafkaSource;

/*
 * @author prashant.bhardwaj@applift.com
 * 
 */

public class KafkaByteSource extends KafkaSource<Schema, byte[]> {

  @Override
  public Extractor<Schema, byte[]> getExtractor(WorkUnitState state) throws IOException {
    return new KafkaByteExtractor(state);
  }
}