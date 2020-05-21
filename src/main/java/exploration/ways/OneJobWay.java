package exploration.ways;

import java.io.File;
import java.nio.file.Files;
import java.util.List;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import exploration.connectors.ApiSource;
import exploration.connectors.MongoSource;

public class OneJobWay implements Way{

	@Override
	public void execute(String[] args) throws Exception {
		
		Files.deleteIfExists(new File("/home/dj/projects/personal/flink-exploration/src/main/java/exploration/data/response-mongo.txt").toPath());
		Files.deleteIfExists(new File("/home/dj/projects/personal/flink-exploration/src/main/java/exploration/data/response-api.txt").toPath());
		
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        
        env
        .addSource(new MongoSource("mongodb://localhost:27017/"))
        .map(new MapFunction<List<String>, List<String>>() {
            @Override
            public List<String> map(List<String> documents) throws Exception {
            	Thread.sleep(2000);
                return documents;
            }
        })
        .writeAsText("file:///home/dj/projects/personal/flink-exploration/src/main/java/exploration/data/response-mongo.txt");

        env
        .addSource(new ApiSource("https://jsonplaceholder.typicode.com/todos/1"))
        .map(new MapFunction<StringBuilder, String>() {
            @Override
            public String map(StringBuilder value) throws Exception {
                return value.toString();
            }
        })
        .map(new MapFunction<String, String>() {
            @Override
            public String map(String value) throws Exception {
            	Thread.sleep(2000);
                return value.toUpperCase();
            }
        })
        .writeAsText("file:///home/dj/projects/personal/flink-exploration/src/main/java/exploration/data/response-api.txt");
        System.out.println(env.getExecutionPlan());

        env.execute("Http Get");
		
	}

}
