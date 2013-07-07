package com.swsandbox;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * User: jgreenwald
 * Date: 7/6/13
 * Time: 11:25 PM
 */
public class HelloWorldReducer extends Reducer<Text, Text, Text, Text>
{
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
    {
        for (Text t : values)
        {
            context.write(key, t);
        }
    }
}
