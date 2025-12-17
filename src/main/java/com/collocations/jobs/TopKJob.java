package com.collocations.jobs;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.Comparator;
import java.util.PriorityQueue;


public class TopKJob {

    public static class TopKMapper extends Mapper<LongWritable, Text, Text, Text> {
        private final Text outKey = new Text(); // lang\tdecade
        private final Text outVal = new Text(); // llr\tw1\tw2

        @Override
        protected void map(LongWritable off, Text line, Context ctx) throws IOException, InterruptedException {
            // lang decade w1 w2 llr
            String[] p = line.toString().split("\t");
            if (p.length < 5) return;
            String lang = p[0];
            String decade = p[1];
            String w1 = p[2];
            String w2 = p[3];
            String llr = p[4];

            outKey.set(lang + "\t" + decade);
            outVal.set(llr + "\t" + w1 + "\t" + w2);
            ctx.write(outKey, outVal);
        }
    }

    public static class TopKReducer extends Reducer<Text, Text, Text, NullWritable> {
        private static class Item {
            final double llr;
            final String w1, w2;
            Item(double llr, String w1, String w2) { this.llr = llr; this.w1 = w1; this.w2 = w2; }
        }

        @Override
        protected void reduce(Text key, Iterable<Text> vals, Context ctx) throws IOException, InterruptedException {
            int K = ctx.getConfiguration().getInt("topk", 100);

            PriorityQueue<Item> pq = new PriorityQueue<>(Comparator.comparingDouble(a -> a.llr)); // min-heap

            for (Text v : vals) {
                String[] p = v.toString().split("\t");
                if (p.length < 3) continue;
                double llr;
                try { llr = Double.parseDouble(p[0]); } catch (Exception e) { continue; }
                String w1 = p[1];
                String w2 = p[2];

                pq.add(new Item(llr, w1, w2));
                if (pq.size() > K)
                    pq.poll(); 
            }

            // output descending
            java.util.ArrayList<Item> out = new java.util.ArrayList<>(pq);
            out.sort((a,b) -> Double.compare(b.llr, a.llr));

            String[] kd = key.toString().split("\t", 2);
            String lang = kd[0], decade = kd[1];

            for (Item it : out) {
                ctx.write(new Text(lang + "\t" + decade + "\t" + it.w1 + "\t" + it.w2 + "\t" + it.llr), NullWritable.get());
            }
        }
    }

    public static void configure(Job job) {
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setMapperClass(TopKMapper.class);
        job.setReducerClass(TopKReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
    }
}
