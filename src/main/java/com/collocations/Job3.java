package com.collocations;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import java.io.IOException;
import java.util.*;
import java.io.DataInput;
import java.io.DataOutput;


    // ==========================================================
    // Job3: Top100 per (lang,decade) with TopK combiner
    // ==========================================================

    /** ScoreWritable to sort/compare easily if needed. */
public class Job3 {

    public static class BigramScore implements Writable {
        public Text bigram = new Text();
        public DoubleWritable score = new DoubleWritable();

        public BigramScore() {}
        public BigramScore(String b, double s) { bigram.set(b); score.set(s); }

        @Override public void write(DataOutput out) throws IOException { bigram.write(out); score.write(out); }
        @Override public void readFields(DataInput in) throws IOException { bigram.readFields(in); score.readFields(in); }
    }

    /** Mapper Job3: parse "lang\tdecade \t bigram\tllr" (as TextOutputFormat gives key+value). */
    public static class Job3Mapper extends Mapper<LongWritable, Text, Text, BigramScore> {
        private final Text outK = new Text();

        @Override
        protected void map(LongWritable off, Text line, Context ctx) throws IOException, InterruptedException {
            // Job2 output line is: key<TAB>value, where key = "lang\tdecade", value="w1 w2\tllr"
            // So full line: lang\tdecade\tw1 w2\tllr
            String[] p = line.toString().split("\t");
            if (p.length < 4) return;
            String lang = p[0];
            String decade = p[1];
            String bigram = p[2];
            double llr;
            try { llr = Double.parseDouble(p[3]); }
            catch (Exception e) { return; }

            outK.set(lang + "\t" + decade);
            ctx.write(outK, new BigramScore(bigram, llr));
        }
    }

    /** TopK helper (min-heap). */
    private static class TopK {
        private final int k;
        private final PriorityQueue<BigramScore> pq;

        TopK(int k) {
            this.k = k;
            this.pq = new PriorityQueue<>(Comparator.comparingDouble(a -> a.score.get()));
        }

        void push(BigramScore s) {
            // copy (Writable reuse safety)
            BigramScore c = new BigramScore(s.bigram.toString(), s.score.get());
            if (pq.size() < k) pq.add(c);
            else if (c.score.get() > pq.peek().score.get()) {
                pq.poll();
                pq.add(c);
            }
        }

        List<BigramScore> sortedDesc() {
            List<BigramScore> out = new ArrayList<>(pq);
            out.sort((a,b) -> Double.compare(b.score.get(), a.score.get()));
            return out;
        }
    }

    /** Combiner Job3: emits local Top100 per key (reduces shuffle massively). */
    public static class Job3Combiner extends Reducer<Text, BigramScore, Text, BigramScore> {
        @Override
        protected void reduce(Text key, Iterable<BigramScore> values, Context ctx) throws IOException, InterruptedException {
            TopK top = new TopK(100);
            for (BigramScore v : values) top.push(v);
            for (BigramScore s : top.sortedDesc()) ctx.write(key, s);
        }
    }

    /** Reducer Job3: merges Top100 per (lang,decade) and outputs final Top100 in descending LLR. */
    public static class Job3Reducer extends Reducer<Text, BigramScore, Text, Text> {
        private final Text outV = new Text();

        @Override
        protected void reduce(Text key, Iterable<BigramScore> values, Context ctx) throws IOException, InterruptedException {
            TopK top = new TopK(100);
            for (BigramScore v : values) top.push(v);

            for (BigramScore s : top.sortedDesc()) {
                // output: lang\tdecade   bigram\tllr
                outV.set(s.bigram.toString() + "\t" + s.score.get());
                ctx.write(key, outV);
            }
        }
    }
}
