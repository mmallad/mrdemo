package com.innovisto.hadoop.mrdemo.email;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by Dipak Malla
 * Date: 6/3/16
 */
public class EmailProcess {
    public static class MappingTask extends Mapper<Object, Text, Text, Text> {
        private Text k = new Text();
        private Text v = new Text();
        //private final static IntWritable one = new IntWritable(1);
        @Override
        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String line = value.toString();
            String[] tokens = line.split(",");
            k.set(tokens[0]);
            v.set(context.getWorkingDirectory().getName());
            context.write(k,v);
        }

    }
    public static class ReducingTask extends Reducer<Text,IntWritable,Text,IntWritable> {
        private IntWritable result = new IntWritable();
        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }
}
