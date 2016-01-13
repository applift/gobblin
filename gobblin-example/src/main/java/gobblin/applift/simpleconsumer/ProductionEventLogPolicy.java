package gobblin.applift.simpleconsumer;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonSyntaxException;

import gobblin.configuration.State;
import gobblin.qualitychecker.row.RowLevelPolicy;
import gobblin.qualitychecker.row.RowLevelPolicy.Result;

public class ProductionEventLogPolicy extends RowLevelPolicy {

	public ProductionEventLogPolicy(State state, Type type) {
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
		JsonObject productionEventObject = element.getAsJsonObject();
		if (productionEventObject == null || productionEventObject.get("timestamp").toString() == null)
			return Result.FAILED;
		return Result.PASSED;
	}
}
