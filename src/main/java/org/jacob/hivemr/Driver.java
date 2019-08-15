package org.jacob.hivemr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.jacob.hivemr.utils.Utils;
import java.util.ArrayList;
import java.util.List;

public class Driver extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Driver(), args);
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {
        GenericOptionsParser optionParser = new GenericOptionsParser(getConf(), args);
        String[] remainingArgs = optionParser.getRemainingArgs();
        if (!(remainingArgs.length == 2 || remainingArgs.length == 4)) {
            System.err
                    .println("Usage: Hive ORC <in> <out> [-skip skipPatternFile]");
            System.exit(2);
        }


        Job job = Job.getInstance(getConf(), "hive orc mapper");
        job.setJarByClass(Driver.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        List<String> otherArgs = new ArrayList<>();
        for (int i = 0; i < remainingArgs.length; ++i) {
            if ("-skip".equals(remainingArgs[i])) {
                job.addCacheFile(new Path(remainingArgs[++i]).toUri());
                job.getConfiguration().setBoolean("wordcount.skip.patterns", true);
            } else {
                otherArgs.add(remainingArgs[i]);
            }
        }

        String input = otherArgs.get(0);
        String output = otherArgs.get(1);

        System.out.println("======================================================");
        System.out.println("== input dir = " + input);
        System.out.println("== output dir = " + output);
        System.out.println("======================================================");

        // Step 1. Delete output dir if exists.
        Utils.deleteDirectory(getConf(), output);

        // Print the conf of all().
        // Utils.printConf(getConf());

        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));

        System.out.println("MapReduce");
        int flag = job.waitForCompletion(true) ? 0 : -1;
        if (flag == 0) {
            System.out.println("^^^^ Job completed Successfully.");
        } else {
            System.out.println("^^^^ Job failed.");
        }
        System.exit(flag);
        return 0;
    }
}
