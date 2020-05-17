package exploration;

import org.apache.flink.streaming.api.functions.source.SourceFunction;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;

public class HttpGetConnection implements SourceFunction<StringBuilder> {

    private final String urlLink;

    public HttpGetConnection(String urlLink) {
        this.urlLink = urlLink;
    }

    @Override
    public void run(SourceContext<StringBuilder> sourceContext) throws Exception {
            URL url = new URL(this.urlLink);
            HttpURLConnection httpconn = null;
            httpconn = (HttpURLConnection) url.openConnection();
            httpconn.setRequestMethod("GET");
            InputStreamReader istr = new InputStreamReader(httpconn.getInputStream());
            BufferedReader br = new BufferedReader(istr);
            StringBuilder sb = new StringBuilder();
            String line;
            while ((line = br.readLine()) != null) {
                sb.append(line + "\n");
            }
            br.close();
            sourceContext.collect(sb);
    }

    @Override
    public void cancel() {
       System.out.println("whaaa");
    }

}