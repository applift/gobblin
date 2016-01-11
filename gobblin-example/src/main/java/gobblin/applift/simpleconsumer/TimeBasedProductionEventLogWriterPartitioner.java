package gobblin.applift.simpleconsumer;


import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import gobblin.configuration.State;
import gobblin.writer.partitioner.TimeBasedWriterPartitioner;

public class TimeBasedProductionEventLogWriterPartitioner extends TimeBasedWriterPartitioner<String> {
	
	public TimeBasedProductionEventLogWriterPartitioner(State state, int numBranches, int branchId) {
		super(state, numBranches, branchId);
	}

	@Override
	public long getRecordTimestamp(String record) {
		JsonElement element = new JsonParser().parse(record);
		JsonObject productionEventObject = element.getAsJsonObject();
		float unixTS = Float.valueOf(productionEventObject.get("timestamp").toString());
		long timestampMS = (long) (unixTS*1000);
		return timestampMS;
	}
}
