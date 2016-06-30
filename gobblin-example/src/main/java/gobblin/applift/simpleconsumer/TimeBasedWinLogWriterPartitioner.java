package gobblin.applift.simpleconsumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gobblin.configuration.State;
import gobblin.writer.partitioner.TimeBasedWriterPartitioner;

public class TimeBasedWinLogWriterPartitioner extends TimeBasedWriterPartitioner<String>{
  private static final Logger LOG = LoggerFactory.getLogger(TimeBasedWinLogWriterPartitioner.class);
  
	public TimeBasedWinLogWriterPartitioner(State state, int numBranches, int branchId) {
		super(state, numBranches, branchId);
	}

	@Override
	public long getRecordTimestamp(String record) {
		String[] columns = record.split("\\|");
		long timestampMS = System.currentTimeMillis();
		try {
		  Double unixTS = Double.valueOf(columns[38])*1000;
		  timestampMS = unixTS.longValue();
    } catch (Exception e) {
      LOG.error("Applift Faulty Record: "+record);
    }
		return timestampMS;
	}
}
