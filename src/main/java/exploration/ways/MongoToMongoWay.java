package exploration.ways;

import java.util.List;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import exploration.connectors.MongoSink;
import exploration.connectors.MongoSource;

public class MongoToMongoWay implements Way{
	
    @Override
    public void execute(String[] args) throws Exception {
    	
    	final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    	
        env
          .addSource(new MongoSource("mongodb://localhost:27017/"))
          .map(new MapFunction<List<String>, List<String>>() {
            @Override
            public List<String> map(List<String> documents) throws Exception {
              return documents;
            }
          })
          .addSink(new MongoSink("mongodb://localhost:27017/"));
        
        env.execute("Mongo");
    }
}

    