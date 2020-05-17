package exploration;

import java.util.HashMap;
import java.util.Map;

public class App {

	public static void main(String[] args) throws Exception {
		Map<Integer, Way> ways = new HashMap<>();
		
		// this was an example by flink
		ways.put(1, new FirstWay());
		ways.put(2, new CsvToCsvWay());
		ways.put(3, new ApiToTextWay());
		ways.put(4, new MongoToMongoWay());
		ways.put(5, new MultiWay());
		ways.put(6, new DependentWay());

		ways.get(6).execute(args);
	}
}
