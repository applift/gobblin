package gobblin.applift.parquet;

import org.apache.avro.generic.GenericRecord;

import gobblin.configuration.State;
import gobblin.writer.partitioner.TimeBasedWriterPartitioner;

/* A time based partitioner which partition record based on system timestamp.
 * 
 * @author prashant.bhardwaj@applift.com
 * 
 */


public class TimeBasedParquetWriterPartitioner extends TimeBasedWriterPartitioner<GenericRecord> {
	public TimeBasedParquetWriterPartitioner(State state, int numBranches, int branchId) {
		super(state, numBranches, branchId);
	}

	@Override
	public long getRecordTimestamp(GenericRecord record) {
	   GenericRecord req_info = (GenericRecord) record.get("req_info");
	   Double timestamp = ((Double)req_info.get("unix_ts"))*1000;
	   return timestamp.longValue();
	}
}
