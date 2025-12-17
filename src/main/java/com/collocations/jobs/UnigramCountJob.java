package collocations.jobs;

import collocations.NgramUtil;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class UnigramCountJob {

    // Output key formats:
    // U \t lang \t decade \t word        -> count
    // T \t lang \t decade                -> partial total (later re-aggregated)
    // lang = language (en or he)
    public static class UnigramMapper extends Mapper<Text, Text, Text, LongWritable> {
        private final Text outKey = new Text();
        private final LongWritable outVal = new LongWritable();
        private String lang;

        @Override
        protected void setup(Context ctx) {
            lang = ctx.getConfiguration().get("lang");
        }

        @Override
        protected void map(Text key, Text value, Context ctx) throws IOException, InterruptedException {
            NgramUtil.Parsed p = NgramUtil.parseKeyValue(key.toString(), value.toString());
            if (p == null) return;

            String word = p.gram();
            int decade = NgramUtil.decadeOfYear(p.year());
            long c = p.matchCount();
            if (c <= 0) return;

            outVal.set(c);

            outKey.set("U\t" + lang + "\t" + decade + "\t" + word);
            ctx.write(outKey, outVal);

            outKey.set("T\t" + lang + "\t" + decade);
            ctx.write(outKey, outVal);
        }
    }

    public static class SumReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
        private final LongWritable sum = new LongWritable();
        @Override
        protected void reduce(Text k, Iterable<LongWritable> vals, Context ctx) throws IOException, InterruptedException {
            long s = 0;
            for (LongWritable v : vals) s += v.get();
            sum.set(s);
            ctx.write(k, sum);
        }
    }

    public static void configure(Job job) {
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setMapperClass(UnigramMapper.class);
        job.setCombinerClass(SumReducer.class);
        job.setReducerClass(SumReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
    }
}
