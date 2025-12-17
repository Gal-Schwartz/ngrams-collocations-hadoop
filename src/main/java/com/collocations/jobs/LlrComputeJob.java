package collocations.jobs;

import collocations.LLR;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.*;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

public class LlrComputeJob {

    // Load N per (lang,decade) from distributed cache file.
    public static class LlrMapper extends Mapper<LongWritable, Text, Text, Text> {
        private final Text outKey = new Text();
        private final Text outVal = new Text();
        private final Map<String, Long> totalsN = new HashMap<>();

        @Override
        protected void setup(Context ctx) throws IOException {
            URI[] cache = ctx.getCacheFiles();
            if (cache == null) return;

            for (URI u : cache) {
                // totals file lines: "lang\tdecade\tN"
                try (BufferedReader br = new BufferedReader(
                        new InputStreamReader(new FileInputStream(new File(u.getPath())), StandardCharsets.UTF_8))) {
                    String line;
                    while ((line = br.readLine()) != null) {
                        String[] p = line.trim().split("\t");
                        if (p.length < 3) continue;
                        String key = p[0] + "\t" + p[1];
                        long N = Long.parseLong(p[2]);
                        totalsN.put(key, N);
                    }
                }
            }
        }

        @Override
        protected void map(LongWritable off, Text line, Context ctx) throws IOException, InterruptedException {
            // P \t lang \t decade \t w1 \t w2 \t c12 \t C1/C2 \t c
            String[] p = line.toString().split("\t");
            if (p.length < 8) return;
            if (!"P".equals(p[0])) return;

            String lang = p[1];
            String decade = p[2];
            String w1 = p[3];
            String w2 = p[4];
            String c12 = p[5];
            String which = p[6];  // C1 or C2
            String c = p[7];

            Long N = totalsN.get(lang + "\t" + decade);
            if (N == null || N <= 0) return;

            // Key by bigram identity
            outKey.set(lang + "\t" + decade + "\t" + w1 + "\t" + w2);

            // pass: c12 + which + c + N
            outVal.set(c12 + "\t" + which + "\t" + c + "\t" + N);
            ctx.write(outKey, outVal);
        }
    }

    public static class LlrReducer extends Reducer<Text, Text, Text, NullWritable> {
        private final Text out = new Text();

        @Override
        protected void reduce(Text key, Iterable<Text> vals, Context ctx) throws IOException, InterruptedException {
            long c1 = -1, c2 = -1, c12 = -1, N = -1;

            for (Text v : vals) {
                String[] p = v.toString().split("\t");
                if (p.length < 4) continue;

                long c12v = Long.parseLong(p[0]);
                String which = p[1];
                long cv = Long.parseLong(p[2]);
                long Nv = Long.parseLong(p[3]);

                c12 = c12v; // same for all
                N = Nv;

                if ("C1".equals(which)) c1 = cv;
                else if ("C2".equals(which)) c2 = cv;
            }

            if (c1 < 0 || c2 < 0 || c12 < 0 || N <= 0) return;

            double llr = LLR.compute(c1, c2, c12, N);
            if (Double.isNaN(llr) || Double.isInfinite(llr)) return;

            // Output: lang decade w1 w2 llr c12 c1 c2 N  (helpful for debugging)
            String[] k = key.toString().split("\t", 4);
            if (k.length < 4) return;

            out.set(k[0] + "\t" + k[1] + "\t" + k[2] + "\t" + k[3] + "\t" + llr);
            ctx.write(out, NullWritable.get());
        }
    }

    public static void configure(Job job) {
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setMapperClass(LlrMapper.class);
        job.setReducerClass(LlrReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
    }
}
