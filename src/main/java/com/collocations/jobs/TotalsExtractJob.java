package collocations.jobs;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;


public class TotalsExtractJob {

    public static class TotalsMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
        private final Text outKey = new Text();
        private final LongWritable outVal = new LongWritable();

        @Override
        protected void map(LongWritable offset, Text line, Context ctx) throws IOException, InterruptedException {
            // input line: "<key>\t<value>"
            // key is: "T\tlang\tdecade" or "U\tlang\tdecade\tword"
            String[] parts = line.toString().split("\t");
            if (parts.length < 2) return;

            if (!"T".equals(parts[0])) return; // only totals
            if (parts.length < 4) return;      // T lang decade <count>

            String lang = parts[1];
            String decade = parts[2];

            long count;
            try { count = Long.parseLong(parts[3]); } catch (Exception e) { return; }
            outKey.set(lang + "\t" + decade);
            outVal.set(count);
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
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setMapperClass(TotalsMapper.class);
        job.setCombinerClass(SumReducer.class);
        job.setReducerClass(SumReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        // totals are tiny – OK to force 1 reducer so it becomes one small file
        job.setNumReduceTasks(1);
    }
}
