package gobblin.applift.simpleconsumer;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonSyntaxException;

import gobblin.configuration.State;
import gobblin.qualitychecker.row.RowLevelPolicy;

public class ReqLogPolicy extends RowLevelPolicy {
	public ReqLogPolicy(State state, Type type) {
		super(state, type);
	}

	@Override
	public Result executePolicy(Object record) {
		String logRecord = (String) record;
		boolean isLogRotate = logRecord.contains("LOGROTATE");
		if (isLogRotate) {
			return Result.FAILED;
		}
		JsonElement element;
		try {
			element = new JsonParser().parse(logRecord);
		} catch (JsonSyntaxException e) {
			return Result.FAILED;
		}
		JsonObject reqLogObject = element.getAsJsonObject();
		JsonObject reqInfoObject = reqLogObject.getAsJsonObject("req_info");
		if (reqInfoObject == null || reqInfoObject.get("unix_ts") == null)
			return Result.FAILED;
		return Result.PASSED;
	}
}
