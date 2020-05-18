package exploration.ways;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.MultipleParameterTool;

public class CsvToCsvWay implements Way{

    @Override
    public void execute(String[] args) throws Exception{

        final MultipleParameterTool params = MultipleParameterTool.fromArgs(args);
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setGlobalJobParameters(params);

        env
        .readCsvFile("file:///home/dj/projects/personal/flink-exploration/src/main/java/exploration/data/request.csv").types(String.class, String.class)
        .map(new MapFunction<Tuple2<String, String>, Tuple2<String, String>>() {
            @Override
            public Tuple2<String, String> map(Tuple2<String, String> value) throws Exception {
                return new Tuple2<>(value.f1, value.f0);
            }
        })
        .writeAsCsv("file:///home/dj/projects/personal/flink-exploration/src/main/java/exploration/data/response.csv");

        env.execute("CSv example");
    }

}