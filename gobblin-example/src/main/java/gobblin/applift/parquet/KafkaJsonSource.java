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

public class KafkaJsonSource extends KafkaSource<Schema, String> {

	@Override
	public Extractor<Schema, String> getExtractor(WorkUnitState state) throws IOException {
		return new KafkaJsonExtractor(state);
	}
}
