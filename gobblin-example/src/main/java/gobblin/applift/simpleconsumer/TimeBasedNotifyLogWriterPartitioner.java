package gobblin.applift.simpleconsumer;

import gobblin.configuration.State;
import gobblin.writer.partitioner.TimeBasedWriterPartitioner;

public class TimeBasedNotifyLogWriterPartitioner extends TimeBasedWriterPartitioner<String> {
	public TimeBasedNotifyLogWriterPartitioner(State state, int numBranches, int branchId) {
		super(state, numBranches, branchId);
	}

	@Override
	public long getRecordTimestamp(String record) {
		String[] columns = record.split("\\|");
		Double unixTS = Double.valueOf(columns[33])*1000;
		long timestampMS = unixTS.longValue();
		return timestampMS;
	}
}
