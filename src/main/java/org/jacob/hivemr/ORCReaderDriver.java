package org.jacob.hivemr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.orc.OrcNewInputFormat;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.jacob.hivemr.mapreduce.ORCReaderMapper;
import org.jacob.hivemr.utils.Utils;

public class ORCReaderDriver extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new ORCReaderDriver(), args);
        System.exit(res);
    }

    @Override
    public int run(String[] arg0) throws Exception {
        Configuration conf = getConf();
//      conf.set("mapreduce.job.inputformat.class", "org.apache.hadoop.hive.ql.io.orc.OrcNewInputFormat");
//      conf.set("mapreduce.input.fileinputformat.inputdir", arg0[0]);
//      conf.set("mapreduce.job.queuename", "platformeng");

        Job job = Job.getInstance(conf,"Read ORC Files");
        job.setJarByClass(ORCReaderDriver.class);
        job.setMapperClass(ORCReaderMapper.class);
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(Text.class);


        job.setOutputFormatClass(TextOutputFormat.class);

        // Step 1. Delete output dir if exists.
        Utils.deleteDirectory(conf, arg0[1]);

        MultipleInputs.addInputPath(job, new Path(arg0[0]), OrcNewInputFormat.class);
        //FileInputFormat.addInputPath(job, new Path(arg0[0]));
        FileInputFormat.setInputDirRecursive(job, true);

        FileOutputFormat.setOutputPath(job, new Path(arg0[1]));
        job.setNumReduceTasks(0);
        System.exit(job.waitForCompletion(true) ?0:1);

        return 0;
    }
}
