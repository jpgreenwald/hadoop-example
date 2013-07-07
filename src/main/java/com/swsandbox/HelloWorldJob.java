package com.swsandbox;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


/**
 * User: jgreenwald
 * Date: 7/6/13
 * Time: 11:22 PM
 */
public class HelloWorldJob extends Configured implements Tool
{
    public static void main(String[] args) throws Exception
    {
        int res = ToolRunner.run(new Configuration(), new HelloWorldJob(), args);
        System.exit(res);
    }

    @Override
    public int run(String[] strings) throws Exception
    {
        Job job = new Job(getConf(), "HelloWorldJob");
        job.setJarByClass(getClass());

        job.setMapperClass(HelloWorldMapper.class);
        job.setReducerClass(HelloWorldReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(strings[0]));
        FileOutputFormat.setOutputPath(job, new Path(strings[1]));

        boolean success = job.waitForCompletion(true);

        return success ? 0 : 1;
    }
}
