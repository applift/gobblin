package gobblin.applift.simpleconsumer;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import gobblin.configuration.State;
import gobblin.writer.partitioner.TimeBasedWriterPartitioner;

public class TimeBasedReqLogWriterPartitioner extends TimeBasedWriterPartitioner<String> {	
	public TimeBasedReqLogWriterPartitioner(State state, int numBranches, int branchId) {
		super(state, numBranches, branchId);
	}

	@Override
	public long getRecordTimestamp(String record) {
		JsonElement element = new JsonParser().parse(record);
		JsonObject reqLogObject = element.getAsJsonObject();
		JsonObject reqInfoObject = reqLogObject.getAsJsonObject("req_info");
		Double unixTS = Double.valueOf(reqInfoObject.get("unix_ts").toString())*1000;
		long timestampMS = unixTS.longValue();
		return timestampMS;
	}
}
