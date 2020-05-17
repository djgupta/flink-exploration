package exploration;

import java.util.List;
import java.util.stream.Collectors;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.bson.Document;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

public class MongoSink extends RichSinkFunction<List<String>>{
	
	private final String connString;
	private MongoClient mongoClient;
	
	public MongoSink(String connString) {
		this.connString = connString;
	}
	
	@Override
    public void invoke(List<String> documents) throws Exception {
    	MongoDatabase database = mongoClient.getDatabase("test");
		MongoCollection<Document> collection = database.getCollection("test");
		collection.insertMany(documents.parallelStream().map(Document :: parse).collect(Collectors.toList()));
    }

    @Override
    public void open(Configuration parameters) throws Exception {
    	ConnectionString connString = new ConnectionString(this.connString);
		MongoClientSettings settings = MongoClientSettings.builder()
		    .applyConnectionString(connString)
		    .retryWrites(true)
		    .build();
		mongoClient = MongoClients.create(settings);
    }

    @Override
    public void close() throws Exception {
    	mongoClient.close();
    	System.out.println("this got cancelled");
    }

}
