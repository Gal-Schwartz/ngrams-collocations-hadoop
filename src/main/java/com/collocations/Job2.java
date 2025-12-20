package com.collocations;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.io.DataInput;
import java.io.DataOutput;


 // ==========================================================
    // Job2: Join on w2 (secondary sort) + compute LLR (streaming)
    // ==========================================================

    /**
     * Job2Key: (lang, decade, w2, sortType, w1)
     * sortType:
     *  0 = UNI(w2) must come first
     *  1 = P(w2,w1,...) after UNI
     */
public class Job2 {

    public static class Job2Key implements WritableComparable<Job2Key> {
        public Text lang = new Text();
        public IntWritable decade = new IntWritable();
        public Text w2 = new Text();
        public ByteWritable sortType = new ByteWritable();
        public Text w1 = new Text();

        public Job2Key() {}

        public Job2Key(String lang, int decade, String w2, byte sortType, String w1) {
            this.lang.set(lang);
            this.decade.set(decade);
            this.w2.set(w2 == null ? "" : w2);
            this.sortType.set(sortType);
            this.w1.set(w1 == null ? "" : w1);
        }

        @Override public void write(DataOutput out) throws IOException {
            lang.write(out); decade.write(out); w2.write(out); sortType.write(out); w1.write(out);
        }
        @Override public void readFields(DataInput in) throws IOException {
            lang.readFields(in); decade.readFields(in); w2.readFields(in); sortType.readFields(in); w1.readFields(in);
        }

        @Override
        public int compareTo(Job2Key o) {
            int c = lang.compareTo(o.lang); if (c != 0) return c;
            c = Integer.compare(decade.get(), o.decade.get()); if (c != 0) return c;
            c = w2.compareTo(o.w2); if (c != 0) return c;
            c = Byte.compare(sortType.get(), o.sortType.get()); if (c != 0) return c;
            return w1.compareTo(o.w1);
        }
    }

    /** Group by (lang,decade,w2). */
    public static class Job2Group extends WritableComparator {
        protected Job2Group() { super(Job2Key.class, true); }
        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            Job2Key x = (Job2Key) a;
            Job2Key y = (Job2Key) b;
            int c = x.lang.compareTo(y.lang); if (c != 0) return c;
            c = Integer.compare(x.decade.get(), y.decade.get()); if (c != 0) return c;
            return x.w2.compareTo(y.w2);
        }
    }

    /** Partition by (lang,decade,w2) to keep Job2 scalable. */
    public static class Job2Partitioner extends Partitioner<Job2Key, Job2Val> {
        @Override
        public int getPartition(Job2Key key, Job2Val value, int numPartitions) {
            int h = 17;
            h = 31 * h + key.lang.toString().hashCode();
            h = 31 * h + key.decade.get();
            h = 31 * h + key.w2.toString().hashCode();
            return (h & Integer.MAX_VALUE) % numPartitions;
        }
    }

    /** Job2Val: carries c2 for UNI or (c12,c1) for P. */
    public static class Job2Val implements Writable {
        public byte tag; // 0=UNI, 1=P
        public LongWritable c2 = new LongWritable();
        public LongWritable c12 = new LongWritable();
        public LongWritable c1 = new LongWritable();

        public Job2Val() {}

        public static Job2Val uni(long c2) {
            Job2Val v = new Job2Val();
            v.tag = 0;
            v.c2.set(c2);
            return v;
        }

        public static Job2Val partial(long c12, long c1) {
            Job2Val v = new Job2Val();
            v.tag = 1;
            v.c12.set(c12);
            v.c1.set(c1);
            return v;
        }

        @Override public void write(DataOutput out) throws IOException {
            out.writeByte(tag);
            c2.write(out); c12.write(out); c1.write(out);
        }
        @Override public void readFields(DataInput in) throws IOException {
            tag = in.readByte();
            c2.readFields(in); c12.readFields(in); c1.readFields(in);
        }
    }

    /**
     * Job2 Mapper:
     * reads Job1 DATA output lines:
     *  UNI \t lang \t decade \t w \t c
     *  P   \t lang \t decade \t w2 \t w1 \t c12 \t c1
     */
    public static class Job2Mapper extends Mapper<LongWritable, Text, Job2Key, Job2Val> {
        @Override
        protected void map(LongWritable off, Text line, Context ctx) throws IOException, InterruptedException {
            String[] p = line.toString().split("\t");
            if (p.length < 1) return;

            String type = p[0];
            if ("UNI".equals(type)) {
                if (p.length < 5) return;
                String lang = p[1];
                int decade = Integer.parseInt(p[2]);
                String w2 = p[3];
                long c2 = Long.parseLong(p[4]);
                ctx.write(new Job2Key(lang, decade, w2, (byte) 0, ""), Job2Val.uni(c2));
            } else if ("P".equals(type)) {
                if (p.length < 7) return;
                String lang = p[1];
                int decade = Integer.parseInt(p[2]);
                String w2 = p[3];
                String w1 = p[4];
                long c12 = Long.parseLong(p[5]);
                long c1 = Long.parseLong(p[6]);
                ctx.write(new Job2Key(lang, decade, w2, (byte) 1, w1), Job2Val.partial(c12, c1));
            }
        }
    }

    /**
     * Job2 Reducer:
     * - Loads N(lang,decade) from distributed cache (Job1 N output)
     * - For each group (lang,decade,w2), gets c2 from UNI first (secondary sort)
     * - Streams P records, computes LLR, outputs:
     *    key:  lang \t decade
     *    value: w1 w2 \t llr
     */

        /** Record-wise streaming reducer to access per-record key (thus w1). */
        public static class Job2ReducerStreaming extends Reducer<Job2Key, Job2Val, Text, Text> {
            private final Map<String, Long> NMap = new HashMap<>();
            private final Text outK = new Text();
            private final Text outV = new Text();

            @Override
            protected void setup(Context context) throws IOException {
                URI[] files = context.getCacheFiles();
                if (files == null) return;

                for (URI u : files) {
                    String localName = (u.getFragment() != null) ? u.getFragment() : new Path(u.getPath()).getName();
                try (BufferedReader br = new BufferedReader(new FileReader(localName))) 
                    {
                        String line;
                        while ((line = br.readLine()) != null) {
                            String[] p = line.split("\t");
                            if (p.length < 4) continue;
                            if (!"N".equals(p[0])) continue;
                            String lang = p[1];
                            String decade = p[2];
                            long N = Long.parseLong(p[3]);
                            NMap.put(lang + "\t" + decade, N);
                        }
                    }
                }
            }

            @Override
            public void run(Context context) throws IOException, InterruptedException {
                setup(context);

                Job2Key currentGroup = null;
                String gLang = null;
                int gDecade = 0;
                String gW2 = null;

                long c2 = 0;
                long N = 0;

                while (context.nextKeyValue()) {
                    Job2Key k = context.getCurrentKey();
                    Job2Val v = context.getCurrentValue();

                    boolean isNewGroup = (currentGroup == null) ||
                            !k.lang.equals(currentGroup.lang) ||
                            k.decade.get() != currentGroup.decade.get() ||
                            !k.w2.equals(currentGroup.w2);

                    if (isNewGroup) {
                        // reset for new group
                        currentGroup = new Job2Key(k.lang.toString(), k.decade.get(), k.w2.toString(), k.sortType.get(), k.w1.toString());
                        gLang = k.lang.toString();
                        gDecade = k.decade.get();
                        gW2 = k.w2.toString();

                        c2 = 0;
                        N = NMap.getOrDefault(gLang + "\t" + gDecade, 0L);
                        if (N <= 0) {
                            // still need to consume records, but we can skip compute
                        }
                    }

                    if (N <= 0) continue;

                    byte t = k.sortType.get();
                    if (t == 0) {
                        // UNI(w2)
                        c2 += v.c2.get();
                    } else {
                        // P record
                        if (c2 <= 0) continue;
                        long c12 = v.c12.get();
                        long c1 = v.c1.get();
                        if (c12 <= 0 || c1 <= 0) continue;

                        double llr = LLR.computeLLR(c1, c2, c12, N);
                        if (Double.isNaN(llr) || Double.isInfinite(llr)) continue;

                        String w1 = k.w1.toString();
                        outK.set(gLang + "\t" + gDecade);
                        outV.set(w1 + " " + gW2 + "\t" + llr);
                        context.write(outK, outV);
                    }
                }

                cleanup(context);
            }
        }
    }
    
