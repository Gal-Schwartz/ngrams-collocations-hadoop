package collocations.jobs;

import collocations.NgramUtil;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.*;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;

public class BigramCountJob {

    // key: B \t lang \t decade \t w1 \t w2 -> count
    public static class BigramMapper extends Mapper<Text, Text, Text, LongWritable> {
        private final Text outKey = new Text();
        private final LongWritable outVal = new LongWritable();
        private final HashSet<String> stop = new HashSet<>();
        private String lang;

        @Override
        protected void setup(Context ctx) throws IOException {
            lang = ctx.getConfiguration().get("lang");
            URI[] cache = ctx.getCacheFiles();
            if (cache != null) {
                for (URI u : cache) {
                    try (BufferedReader br = new BufferedReader(
                            new InputStreamReader(new FileInputStream(new File(u.getPath())), StandardCharsets.UTF_8))) {
                        String s;
                        while ((s = br.readLine()) != null) {
                            s = s.trim();
                            if (!s.isEmpty() && !s.startsWith("#")) stop.add(s);
                        }
                    }
                }
            }
        }

        @Override
        protected void map(Text key, Text value, Context ctx) throws IOException, InterruptedException {
            NgramUtil.Parsed p = NgramUtil.parseKeyValue(key.toString(), value.toString());
            if (p == null) return;

            String[] w = p.gram().split(" ");
            if (w.length != 2) return;

            String w1 = w[0];
            String w2 = w[1];
            if (stop.contains(w1) || stop.contains(w2)) return;

            int decade = NgramUtil.decadeOfYear(p.year());
            long c12 = p.matchCount();
            if (c12 <= 0) return;

            outKey.set("B\t" + lang + "\t" + decade + "\t" + w1 + "\t" + w2);
            outVal.set(c12);
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

        job.setMapperClass(BigramMapper.class);
        job.setCombinerClass(SumReducer.class);
        job.setReducerClass(SumReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
    }
}
