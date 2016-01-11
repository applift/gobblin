package gobblin.applift.simpleconsumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gobblin.configuration.State;
import gobblin.writer.partitioner.TimeBasedWriterPartitioner;

public class TimeBasedNotifyLogWriterPartitioner extends TimeBasedWriterPartitioner<String> {
	public TimeBasedNotifyLogWriterPartitioner(State state, int numBranches, int branchId) {
		super(state, numBranches, branchId);
	}

	@Override
	public long getRecordTimestamp(String record) {
		String[] columns = record.split("\\|");
		float unixTS = Float.valueOf(columns[32]);
		long timestampMS = (long) (unixTS * 1000);
		return timestampMS;
	}

}
