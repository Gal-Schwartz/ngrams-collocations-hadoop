package com.collocations;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class CollocationExtraction3Jobs extends Configured implements Tool {

     @Override
    public int run(String[] args) throws Exception {
        if (args.length < 7) {
            System.err.println("Usage:\n" +
                    "CollocationExtraction3Jobs <eng2> <eng1> <heb2> <heb1> <stop_en> <stop_he> <out>");
            return 2;
        }

        String eng2 = args[0];
        String eng1 = args[1];
        String heb2 = args[2];
        String heb1 = args[3];
        String stopEn = args[4];
        String stopHe = args[5];
        String out = args[6];

        String job1Root = out + "/tmp_job1";
        String job1Data = job1Root + "/data";
        String job1N = job1Root + "/N";
        String job2Out = out + "/tmp_job2";
        String finalOut = out + "/final";

        Configuration conf = getConf();

        // ---------------- Job1 ----------------
        Job job1 = Job.getInstance(conf, "Job1-JoinW1-Streaming-SecondarySort");
        job1.setJarByClass(CollocationExtraction3Jobs.class);

        job1.addCacheFile(new URI(stopEn));
        job1.addCacheFile(new URI(stopHe));

        job1.setMapOutputKeyClass(Job1.Job1Key.class);
        job1.setMapOutputValueClass(Job1.Job1Val.class);
        job1.setPartitionerClass(Job1.Job1Partitioner.class);
        job1.setGroupingComparatorClass(Job1.Job1Group.class);

        job1.setCombinerClass(Job1.Job1Combiner.class);
        job1.setReducerClass(Job1.Job1ReducerStreaming.class);

        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);

        // MultipleOutputs: two named outputs (DATA + N)
        MultipleOutputs.addNamedOutput(job1, Constants.MO_DATA, TextOutputFormat.class, Text.class, Text.class);
        MultipleOutputs.addNamedOutput(job1, Constants.MO_N, TextOutputFormat.class, Text.class, Text.class);

        // Inputs: SequenceFiles
        MultipleInputs.addInputPath(job1, new Path(eng1), SequenceFileInputFormat.class, Job1.Job1UnigramMapper.class);
        MultipleInputs.addInputPath(job1, new Path(heb1), SequenceFileInputFormat.class, Job1.Job1UnigramMapper.class);
        MultipleInputs.addInputPath(job1, new Path(eng2), SequenceFileInputFormat.class, Job1.Job1BigramMapper.class);
        MultipleInputs.addInputPath(job1, new Path(heb2), SequenceFileInputFormat.class, Job1.Job1BigramMapper.class);

        // Base output path for MultipleOutputs
        FileOutputFormat.setOutputPath(job1, new Path(job1Root));

        if (!job1.waitForCompletion(true)) return 1;

        // ---------------- Job2 ----------------
        Job job2 = Job.getInstance(conf, "Job2-JoinW2-LLR-Streaming-SecondarySort");
        job2.setJarByClass(CollocationExtraction3Jobs.class);

        // Add Job1 N output files to distributed cache for Job2
        addAllPartFilesToCache(job2, new Path(job1N));

        job2.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job2, new Path(job1Data));
        FileOutputFormat.setOutputPath(job2, new Path(job2Out));

        job2.setMapperClass(Job2.Job2Mapper.class);
        job2.setMapOutputKeyClass(Job2.Job2Key.class);
        job2.setMapOutputValueClass(Job2.Job2Val.class);

        job2.setPartitionerClass(Job2.Job2Partitioner.class);
        job2.setGroupingComparatorClass(Job2.Job2Group.class);
        job2.setReducerClass(Job2.Job2ReducerStreaming.class);

        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);

        if (!job2.waitForCompletion(true)) return 1;

        // ---------------- Job3 ----------------
        Job job3 = Job.getInstance(conf, "Job3-Top100-PerLangDecade-WithCombiner");
        job3.setJarByClass(CollocationExtraction3Jobs.class);

        job3.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job3, new Path(job2Out));
        FileOutputFormat.setOutputPath(job3, new Path(finalOut));

        job3.setMapperClass(Job3.Job3Mapper.class);
        job3.setMapOutputKeyClass(Text.class);
        job3.setMapOutputValueClass(Job3.BigramScore.class);

        job3.setCombinerClass(Job3.Job3Combiner.class);
        job3.setReducerClass(Job3.Job3Reducer.class);

        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(Text.class);

        return job3.waitForCompletion(true) ? 0 : 1;
    }

    private static void addAllPartFilesToCache(Job job, Path dir) throws IOException {
        Configuration conf = job.getConfiguration();
        FileSystem fs = dir.getFileSystem(conf);
        if (!fs.exists(dir)) return;

        for (FileStatus st : fs.listStatus(dir)) {
            Path p = st.getPath();
            String name = p.getName();
            if (st.isFile() && name.startsWith("part-")) {
                job.addCacheFile(p.toUri());
            }
        }
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new Configuration(), new CollocationExtraction3Jobs(), args));
    }
}
