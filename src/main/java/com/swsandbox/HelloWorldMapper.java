package com.swsandbox;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * User: jgreenwald
 * Date: 7/6/13
 * Time: 11:24 PM
 */
public class HelloWorldMapper extends Mapper<LongWritable, Text, Text, Text>
{
    @Override
    protected void map(LongWritable writable, Text value, Context context) throws IOException, InterruptedException
    {
        context.write(value, new Text(value + " is being emitted"));
    }
}
