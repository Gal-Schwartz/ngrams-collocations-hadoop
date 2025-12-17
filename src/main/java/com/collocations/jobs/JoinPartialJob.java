package com.collocations.jobs;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class JoinPartialJob {

    // From unigram output lines:
    // U \t lang \t decade \t word \t c
    public static class UnigramSideMapper extends Mapper<LongWritable, Text, Text, Text> {
        private final Text outKey = new Text();
        private final Text outVal = new Text();

        @Override
        protected void map(LongWritable off, Text line, Context ctx) throws IOException, InterruptedException {
            String[] p = line.toString().split("\t");
            if (p.length < 5) return;
            if (!"U".equals(p[0])) return;

            String lang = p[1];
            String decade = p[2];
            String word = p[3];
            String c = p[4];

            outKey.set(lang + "\t" + decade + "\t" + word);
            outVal.set("UNI\t" + c);
            ctx.write(outKey, outVal);
        }
    }

    // From bigram output lines:
    // B \t lang \t decade \t w1 \t w2 \t c12
    public static class BigramSideMapper extends Mapper<LongWritable, Text, Text, Text> {
        private final Text outKey = new Text();
        private final Text outVal = new Text();

        @Override
        protected void map(LongWritable off, Text line, Context ctx) throws IOException, InterruptedException {
            String[] p = line.toString().split("\t");
            if (p.length < 6) return;
            if (!"B".equals(p[0])) return;

            String lang = p[1];
            String decade = p[2];
            String w1 = p[3];
            String w2 = p[4];
            String c12 = p[5];

            // Emit request for w1
            outKey.set(lang + "\t" + decade + "\t" + w1);
            outVal.set("REQ_L\t" + w1 + "\t" + w2 + "\t" + c12);
            ctx.write(outKey, outVal);

            // Emit request for w2
            outKey.set(lang + "\t" + decade + "\t" + w2);
            outVal.set("REQ_R\t" + w1 + "\t" + w2 + "\t" + c12);
            ctx.write(outKey, outVal);
        }
    }

    // Output:
    // P \t lang \t decade \t w1 \t w2 \t c12 \t c1_or_c2_tag \t c
    public static class JoinReducer extends Reducer<Text, Text, Text, NullWritable> {
        private final Text out = new Text();

        @Override
        protected void reduce(Text key, Iterable<Text> vals, Context ctx) throws IOException, InterruptedException {
            Long unigramCount = null;

            // We must stream; values are iterable, but we can do 2 passes only if we buffer.
            // Instead: first find unigramCount; if none, still can’t compute.
            // We will store requests temporarily on disk? Not allowed.
            // Solution: buffer ONLY the requests for THIS word in memory.
            // This does not violate constraints (it’s per word, not per decade).
            java.util.ArrayList<String> reqs = new java.util.ArrayList<>();

            for (Text v : vals) {
                String s = v.toString();
                if (s.startsWith("UNI\t")) {
                    unigramCount = Long.parseLong(s.substring(4));
                } else {
                    reqs.add(s);
                }
            }
            if (unigramCount == null) return;

            // key = lang decade word
            String[] k = key.toString().split("\t", 3);
            if (k.length < 3) return;
            String lang = k[0];
            String decade = k[1];
            String word = k[2];

            for (String r : reqs) {
                String[] p = r.split("\t");
                // p[0] = REQ_L / REQ_R, p[1]=w1, p[2]=w2, p[3]=c12
                if (p.length < 4) continue;
                String side = p[0];
                String w1 = p[1];
                String w2 = p[2];
                String c12 = p[3];

                String tag = side.equals("REQ_L") ? "C1" : "C2";
                out.set("P\t" + lang + "\t" + decade + "\t" + w1 + "\t" + w2 + "\t" + c12 + "\t" + tag + "\t" + unigramCount);
                ctx.write(out, NullWritable.get());
            }
        }
    }

    public static void configure(Job job, String unigramsPath, String bigramsPath) throws Exception {
        job.setOutputFormatClass(TextOutputFormat.class);

        MultipleInputs.addInputPath(job, new org.apache.hadoop.fs.Path(unigramsPath), TextInputFormat.class, UnigramSideMapper.class);
        MultipleInputs.addInputPath(job, new org.apache.hadoop.fs.Path(bigramsPath), TextInputFormat.class, BigramSideMapper.class);

        job.setReducerClass(JoinReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
    }
}
