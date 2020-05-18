package exploration.connectors;

import java.util.ArrayList;
import java.util.List;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.bson.Document;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

public class MongoSource extends RichSourceFunction<List<String>> {
	
	private final String connString;
	private MongoClient mongoClient;
	
	public MongoSource(String connString) {
		this.connString = connString;
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
	public void run(SourceContext<List<String>> ctx) throws Exception {
		MongoDatabase database = mongoClient.getDatabase("nse");
		MongoCollection<Document> collection = database.getCollection("exchange_securities");
		
		List<String> documents= new ArrayList<>();
		for(Document doc : collection.find()) {
			documents.add(doc.toJson());
		}
		ctx.collect(documents);
	}

	@Override
	public void cancel() {
		mongoClient.close();
	}

}
