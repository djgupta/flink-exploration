package exploration.ways;

import java.io.File;
import java.nio.file.Files;
import java.util.List;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import exploration.connectors.ApiSource;
import exploration.connectors.MongoSource;

public class MultiJobWay implements Way {

	@Override
	public void execute(String[] args) throws Exception {
		
		Files.deleteIfExists(new File("/home/dj/projects/personal/flink-exploration/src/main/java/exploration/data/response-mongo.txt").toPath());
		Files.deleteIfExists(new File("/home/dj/projects/personal/flink-exploration/src/main/java/exploration/data/response-api.txt").toPath());
		
		final StreamExecutionEnvironment env1 = StreamExecutionEnvironment.getExecutionEnvironment();
		
		env1
        .addSource(new MongoSource("mongodb://localhost:27017/"))
        .map(new MapFunction<List<String>, List<String>>() {
    		@Override
    		public List<String> map(List<String> documents) throws Exception {
    			Thread.sleep(2000);
    			return documents;
    		}
        })
        .writeAsText("file:///home/dj/projects/personal/flink-exploration/src/main/java/exploration/data/response-mongo.txt");
        
		
		final StreamExecutionEnvironment env2 = StreamExecutionEnvironment.getExecutionEnvironment();
        env2
        .addSource(new ApiSource("https://jsonplaceholder.typicode.com/todos/1"))
        .map(new MapFunction<StringBuilder, String>() {
            @Override
            public String map(StringBuilder value) throws Exception {
            	Thread.sleep(2000);
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
        
        System.out.println(env1.getExecutionPlan());
        System.out.println(env2.getExecutionPlan());
        
        env1.execute("Mongo Job");
        env2.execute("API job");
	}

}
