package com.innovisto.hadoop.mrdemo.email;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

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
            //Fifth Position
            String[] tokens = line.split(",");
            if(tokens.length < 2) return;
            String tableName = ((FileSplit) context.getInputSplit()).getPath().getName();
            k.set(tokens[0]);
            if(tableName.equals("p.csv")){
                //1th
                v.set(tokens[1]+"#@$%^p");
            }else if(tableName.equals("e.csv")){
                //5th
                v.set(tokens[5]+"#@$%^e");
            }
            context.write(k,v);
        }

    }
    public static class ReducingTask extends Reducer<Text,IntWritable,Text,Text> {
        private Text result = new Text();
        private Text k = new Text();
        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            String v = "";
            for (Text val : values) {
                String[] s = v.split("#@$%^e");
                if(s[1].equals("p")){
                    k.set(s[1]);
                }else{
                    v += val.toString()+", ";
                }
            }
            //result.set(sum);
            context.write(k, result);
        }
    }
}
