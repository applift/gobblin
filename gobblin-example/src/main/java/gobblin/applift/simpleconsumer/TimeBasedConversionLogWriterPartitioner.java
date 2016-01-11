package gobblin.applift.simpleconsumer;


import gobblin.configuration.State;
import gobblin.writer.partitioner.TimeBasedWriterPartitioner;

public class TimeBasedConversionLogWriterPartitioner extends TimeBasedWriterPartitioner<String> {
	public TimeBasedConversionLogWriterPartitioner(State state, int numBranches, int branchId) {
		super(state, numBranches, branchId);
	}

	@Override
	public long getRecordTimestamp(String record) {
		String[] columns = record.split("\\|");
		float unixTS = Float.valueOf(columns[1]);
		long timestampMS = (long) (unixTS * 1000);
		return timestampMS;
	}

}
