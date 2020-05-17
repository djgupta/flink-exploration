package exploration;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.MultipleParameterTool;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;


public class FirstWay implements Way {

    @Override
    public void execute(String[] args) throws Exception {
        final MultipleParameterTool params = MultipleParameterTool.fromArgs(args);
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		env.getConfig().setGlobalJobParameters(params);

		DataSet<String> text = null;
		if (params.has("input")) {
			for (String input : params.getMultiParameterRequired("input")) {
				if (text == null) {
					text = env.readTextFile(input);
				} else {
					text = text.union(env.readTextFile(input));
				}
			}
			Preconditions.checkNotNull(text, "Input DataSet should not be null.");
		} else {
			text = WordCountData.getDefaultTextLineDataSet(env);
		}

		DataSet<Tuple2<String, Integer>> counts =
				text.flatMap(new Tokenizer())
				.groupBy(0)
				.sum(1);

		if (params.has("output")) {
			counts.writeAsCsv(params.get("output"), "\n", " ");
			env.execute("WordCount Example");
		} else {
			System.out.println("Printing result to stdout. Use --output to specify output path.");
			counts.print();
		}

    }

    public static final class Tokenizer implements FlatMapFunction<String, Tuple2<String, Integer>> {

		@Override
		public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
			String[] tokens = value.toLowerCase().split("\\W+");

			for (String token : tokens) {
				if (token.length() > 0) {
					out.collect(new Tuple2<>(token, 1));
				}
			}
		}
	}

}