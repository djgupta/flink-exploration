package exploration;

import java.util.List;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class MultiWay implements Way {

	@Override
	public void execute(String[] args) throws Exception {
		
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		
		env
        .addSource(new MongoConnection("mongodb://localhost:27017/"))
        .map(new MapFunction<List<String>, List<String>>() {
    		@Override
    		public List<String> map(List<String> documents) throws Exception {
    			return documents;
    		}
        })
//        .addSink(new MongoSink("mongodb://localhost:27017/"));
        .writeAsText("file:///home/dj/projects/personal/flink-exploration/src/main/java/exploration/data/response-mongo.txt");
		
        String url = "https://jsonplaceholder.typicode.com/todos/1";

        env
        .addSource(new HttpGetConnection(url))
        .map(new MapFunction<StringBuilder, String>() {
            @Override
            public String map(StringBuilder value) throws Exception {
                return value.toString();
            }
        })
        .map(new MapFunction<String, String>() {
            @Override
            public String map(String value) throws Exception {
                return value.toUpperCase();
            }
        })
        .writeAsText("file:///home/dj/projects/personal/flink-exploration/src/main/java/exploration/data/response-api.txt");
        
        
		
		
        
        env.execute("Mongo and API");
		
	}

}
