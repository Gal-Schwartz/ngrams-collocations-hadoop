package com.collocations;  
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;


import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


    // ==========================================================
    // Job1: Join on w1 (secondary sort) - STREAMING
    // ==========================================================

    /**
     * Job1Key: (lang, decade, w1, typeOrder, w2)
     * typeOrder:
     *  0 = UNI (c1)  (must come first)
     *  1 = BIGRAM (w1 w2) (after UNI)
     */
public class Job1 {

    public static class Job1Key implements WritableComparable<Job1Key> {
        public Text lang = new Text();
        public IntWritable decade = new IntWritable();
        public Text w1 = new Text();
        public ByteWritable typeOrder = new ByteWritable();
        public Text w2 = new Text();

        public Job1Key() {}

        public Job1Key(String lang, int decade, String w1, byte typeOrder, String w2) {
            this.lang.set(lang);
            this.decade.set(decade);
            this.w1.set(w1 == null ? "" : w1);
            this.typeOrder.set(typeOrder);
            this.w2.set(w2 == null ? "" : w2);
        }

        @Override 
        public void write(DataOutput out) throws IOException {
            lang.write(out); decade.write(out); w1.write(out); typeOrder.write(out); w2.write(out);
        }
        @Override 
        public void readFields(DataInput in) throws IOException {
            lang.readFields(in); decade.readFields(in); w1.readFields(in); typeOrder.readFields(in); w2.readFields(in);
        }

        @Override
        public int compareTo(Job1Key o) {
            int c = lang.compareTo(o.lang); if (c != 0) return c;
            c = Integer.compare(decade.get(), o.decade.get()); if (c != 0) return c;
            c = w1.compareTo(o.w1); if (c != 0) return c;
            c = Byte.compare(typeOrder.get(), o.typeOrder.get()); if (c != 0) return c;
            return w2.compareTo(o.w2);
        }
    }

    /** Group by (lang,decade,w1) instead (lang,decade,w1, typeOrder, w2) */
    public static class Job1Group extends WritableComparator {
        protected Job1Group() { super(Job1Key.class, true); }
        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            Job1Key x = (Job1Key) a;
            Job1Key y = (Job1Key) b;
            int c = x.lang.compareTo(y.lang); if (c != 0) return c;
            c = Integer.compare(x.decade.get(), y.decade.get()); if (c != 0) return c;
            return x.w1.compareTo(y.w1);
        }
    }

    public static class Job1Partitioner extends Partitioner<Job1Key, Job1Val> {
        @Override
        public int getPartition(Job1Key key, Job1Val value, int numPartitions) {
            int h = 17;
            h = 31 * h + key.lang.toString().hashCode();
            h = 31 * h + key.decade.get();
            h = 31 * h + key.w1.toString().hashCode();
            return (h & Integer.MAX_VALUE) % numPartitions;
        }
    }

    /** Job1Val: tag(0=UNI,1=BIGRAM) + count. */
    public static class Job1Val implements Writable {
        public byte tag;
        public LongWritable count = new LongWritable();

        public Job1Val() {}
        public Job1Val(byte tag, long c) { this.tag = tag; this.count.set(c); }

        @Override public void write(DataOutput out) throws IOException {
            out.writeByte(tag); count.write(out);
        }
        @Override public void readFields(DataInput in) throws IOException {
            tag = in.readByte(); count.readFields(in);
        }
    }

    /** Mapper for unigrams: emits UNI (w1=candidate) and contributes to N (via reducer side-output). */
    public static class Job1UnigramMapper extends Mapper<LongWritable, Text, Job1Key, Job1Val> {
        private Stopwords stop;

        @Override protected void setup(Context ctx) throws IOException {
            stop = new Stopwords();
            stop.loadFromCache(ctx);
            System.err.println("Job1: UnigramMapper setup complete. Loaded Stopwords.");
        }

        @Override
        protected void map(LongWritable off, Text line, Context ctx) throws IOException, InterruptedException {
            String lang = NGramUtils.inferLangFromPath(ctx.getInputSplit());
            String[] p = line.toString().split("\t");
            if (p.length < 3) return;

            String wRaw = p[0];
            int year;
            long occ;
            try {
                year = Integer.parseInt(p[1].trim());
                occ = Long.parseLong(p[2].trim());
            } catch (Exception e) {
                return;
            }
            if (occ <= 0) return;

            int decade = NGramUtils.toDecade(year);
            String w = NGramUtils.cleanToken(wRaw);
            if (w.isEmpty()) return;

            // IMPORTANT: stopwords must NOT be included in counts (c1,c2,N)
            if (stop.isStop(lang, w)) return;

            // UNI: key (lang,decade,w1, type=0), value tag=0
            ctx.write(new Job1Key(lang, decade, w, (byte) 0, ""), new Job1Val((byte) 0, occ));
        }
    }

    /** Mapper for bigrams: emits BIGRAM under key with w1, and carries w2 in the key. */
    public static class Job1BigramMapper extends Mapper<LongWritable, Text, Job1Key, Job1Val> {
        private Stopwords stop;

        @Override protected void setup(Context ctx) throws IOException {
            stop = new Stopwords();
            stop.loadFromCache(ctx);
        }

        @Override
        protected void map(LongWritable off, Text line, Context ctx) throws IOException, InterruptedException {
            String lang = NGramUtils.inferLangFromPath(ctx.getInputSplit());
             String[] p = line.toString().split("\t");
        if (p.length < 3) return;

        String word = p[0];
        String[] words = word.split("\\s+");
        if (words.length < 2) return;

        int year;
        long occ;
        try {
            year = Integer.parseInt(p[1].trim());
            occ = Long.parseLong(p[2].trim());
        } catch (Exception e) {
            return;
        }
        if (occ <= 0) return;
            int decade = NGramUtils.toDecade(year);

            String w1 = NGramUtils.cleanToken(words[0]);
            String w2 = NGramUtils.cleanToken(words[1]);
            if (w1.isEmpty() || w2.isEmpty()) return;

            // Remove all bigrams that contain stopwords (and do not include them in counts)
            if (stop.isStop(lang, w1) || stop.isStop(lang, w2)) return;

            // BIGRAM: type=1 comes after UNI in sort, w2 stored in key
            ctx.write(new Job1Key(lang, decade, w1, (byte) 1, w2), new Job1Val((byte) 1, occ));
        }
    }

    /** Combiner for Job1: sums counts per identical key. */
    public static class Job1Combiner extends Reducer<Job1Key, Job1Val, Job1Key, Job1Val> {
        @Override
        protected void reduce(Job1Key key, Iterable<Job1Val> vals, Context ctx) throws IOException, InterruptedException {
            long sum = 0;
            byte tag = -1;
            for (Job1Val v : vals) {
                sum += v.count.get();
                tag = v.tag;
            }
            if (tag == -1) tag = key.typeOrder.get();
            ctx.write(key, new Job1Val(tag, sum));
        }
    }

    /**
     * Job1 reducer (streaming via nextKeyValue):
     * For each group (lang,decade,w1):
     *  - First accumulate c1 from UNI records
     *  - Then stream out partials for each (w1,w2): P line includes c12 and c1
     * Also outputs:
     *  - UNI lines (lang,decade,w,c) to DATA
     *  - N lines (lang,decade,N) to N output (computed as sum of all UNI counts per (lang,decade))
     *
     * N is computed as sum of all unigrams after stopword filtering (consistent with requirement).
     */
    public static class Job1ReducerStreaming extends Reducer<Job1Key, Job1Val, Text, Text> {
        private MultipleOutputs<Text, Text> mos;

        // N accumulators per reducer (small: only decades in input)
        private final Map<String, Long> NByLangDecade = new HashMap<>();

        private final Text outK = new Text();
        private final Text outV = new Text();

        @Override
        protected void setup(Context context) {
            mos = new MultipleOutputs<>(context);
        }

        @Override
        public void run(Context context) throws IOException, InterruptedException {
            setup(context);

            Job1Key currentGroup = null;
            String gLang = null;
            int gDecade = 0;
            String gW1 = null;

            long c1 = 0;

            while (context.nextKeyValue()) {
                Job1Key k = context.getCurrentKey();
                Job1Val v = context.getCurrentValue();

                boolean isNewGroup = (currentGroup == null) ||
                        !k.lang.equals(currentGroup.lang) ||
                        k.decade.get() != currentGroup.decade.get() ||
                        !k.w1.equals(currentGroup.w1);

                if (isNewGroup) {
                    // close previous group: emit UNI (c1)
                    if (currentGroup != null && c1 > 0) {
                        // DATA: UNI \t lang \t decade \t w \t c
                        outK.set("UNI");
                        outV.set(gLang + "\t" + gDecade + "\t" + gW1 + "\t" + c1);
                        mos.write(Constants.MO_DATA, outK, outV, "data/part");

                        // update N for that (lang,decade)
                        String ld = gLang + "\t" + gDecade;
                        NByLangDecade.put(ld, NByLangDecade.getOrDefault(ld, 0L) + c1);
                    }

                    // reset
                    currentGroup = new Job1Key(k.lang.toString(), k.decade.get(), k.w1.toString(), k.typeOrder.get(), k.w2.toString());
                    gLang = k.lang.toString();
                    gDecade = k.decade.get();
                    gW1 = k.w1.toString();
                    c1 = 0;
                }

                // within group:
                byte type = k.typeOrder.get();
                if (type == 0) {
                    // UNI
                    c1 += v.count.get();
                } else if (type == 1) {
                    // BIGRAM: can stream only after UNI has been seen (secondary sort ensures UNI first)
                    if (c1 <= 0) continue;
                    String w2 = k.w2.toString();
                    long c12 = v.count.get();

                    // DATA: P \t lang \t decade \t w2 \t w1 \t c12 \t c1
                    outK.set("P");
                    outV.set(gLang + "\t" + gDecade + "\t" + w2 + "\t" + gW1 + "\t" + c12 + "\t" + c1);
                    mos.write(Constants.MO_DATA, outK, outV, "data/part");
                }
            }

            // close last group
            if (currentGroup != null && c1 > 0) {
                outK.set("UNI");
                outV.set(gLang + "\t" + gDecade + "\t" + gW1 + "\t" + c1);
                mos.write(Constants.MO_DATA, outK, outV, "data/part");

                String ld = gLang + "\t" + gDecade;
                NByLangDecade.put(ld, NByLangDecade.getOrDefault(ld, 0L) + c1);
            }

            // emit N side output (small)
            for (Map.Entry<String, Long> e : NByLangDecade.entrySet()) {
                // N \t lang \t decade \t N
                outK.set("N");
                outV.set(e.getKey() + "\t" + e.getValue());
                mos.write(Constants.MO_N, outK, outV, "N/part");
            }

            cleanup(context);
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            if (mos != null) mos.close();
            super.cleanup(context);
        }
    }
}
