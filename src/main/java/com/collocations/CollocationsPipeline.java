package collocations;

import collocations.jobs.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class CollocationsPipeline {

    public static void main(String[] args) throws Exception {
        // args:
        // 0 lang (en/he)
        // 1 unigramInput
        // 2 bigramInput
        // 3 stopwordsPath (HDFS/S3)
        // 4 outputBase
        if (args.length < 5) {
            System.err.println("Usage: CollocationsPipeline <lang> <unigramInput> <bigramInput> <stopwords> <outputBase>");
            System.exit(1);
        }

        String lang = args[0];
        String unigramInput = args[1];
        String bigramInput = args[2];
        String stopwords = args[3];
        String outBase = args[4];

        Configuration conf = new Configuration();
        conf.set("lang", lang);

        String step1 = outBase + "/01_unigrams";
        String step1b = outBase + "/01b_totals";
        String step2 = outBase + "/02_bigrams";
        String step3 = outBase + "/03_join_partial";
        String step4 = outBase + "/04_llr";
        String step5 = outBase + "/05_top100";

        // Job 1: Unigram counts + (T lang decade) partial totals
        Job j1 = Job.getInstance(conf, "unigram-count");
        j1.setJarByClass(CollocationsPipeline.class);
        UnigramCountJob.configure(j1);
        FileInputFormat.addInputPath(j1, new Path(unigramInput));
        FileOutputFormat.setOutputPath(j1, new Path(step1));
        if (!j1.waitForCompletion(true)) System.exit(2);

        // Job 1b: Extract totals into one small file: lang decade N
        Job j1b = Job.getInstance(conf, "totals-extract");
        j1b.setJarByClass(CollocationsPipeline.class);
        TotalsExtractJob.configure(j1b);
        FileInputFormat.addInputPath(j1b, new Path(step1));
        FileOutputFormat.setOutputPath(j1b, new Path(step1b));
        if (!j1b.waitForCompletion(true)) System.exit(3);

        // Job 2: Bigram count with stopwords filter
        Job j2 = Job.getInstance(conf, "bigram-count");
        j2.setJarByClass(CollocationsPipeline.class);
        BigramCountJob.configure(j2);
        j2.addCacheFile(new Path(stopwords).toUri());
        FileInputFormat.addInputPath(j2, new Path(bigramInput));
        FileOutputFormat.setOutputPath(j2, new Path(step2));
        if (!j2.waitForCompletion(true)) System.exit(4);

        // Job 3: Reduce-side join -> partial records
        Job j3 = Job.getInstance(conf, "join-partial");
        j3.setJarByClass(CollocationsPipeline.class);
        JoinPartialJob.configure(j3, step1, step2);
        FileOutputFormat.setOutputPath(j3, new Path(step3));
        if (!j3.waitForCompletion(true)) System.exit(5);

        // Job 4: Compute LLR (needs totals cache)
        Job j4 = Job.getInstance(conf, "llr-compute");
        j4.setJarByClass(CollocationsPipeline.class);
        LlrComputeJob.configure(j4);
        // totals output (single reducer) will be something like part-r-00000
        j4.addCacheFile(new Path(step1b + "/part-r-00000").toUri());
        FileInputFormat.addInputPath(j4, new Path(step3));
        FileOutputFormat.setOutputPath(j4, new Path(step4));
        if (!j4.waitForCompletion(true)) System.exit(6);

        // Job 5: Top-100 per (lang, decade)
        Job j5 = Job.getInstance(conf, "top100");
        j5.setJarByClass(CollocationsPipeline.class);
        TopKJob.configure(j5);
        j5.getConfiguration().setInt("topk", 100);
        FileInputFormat.addInputPath(j5, new Path(step4));
        FileOutputFormat.setOutputPath(j5, new Path(step5));
        if (!j5.waitForCompletion(true)) System.exit(7);

        System.out.println("DONE. Final output: " + step5);
    }
}
