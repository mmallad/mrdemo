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
            if(tableName.equals("p.csv")){
                //1th
                k.set(tokens[0]);
                v.set(tokens[1]+"^p");
            }else if(tableName.equals("e.csv")){
                //5th
                k.set(tokens[0]);
                v.set(tokens[11]+"^e");
            }
            context.write(k,v);
        }

    }
    public static class ReducingTask extends Reducer<Text,Text,Text,Text> {
        private Text result = new Text();
        private Text k = new Text();
        String tempKey = "";
        StringBuilder v = new StringBuilder();
        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            int i= 0;
            for (Text val : values) {
                i++;
                String record = val.toString();
                String[] records = record.split("^");
                if(records.length != 2) continue;
                if(records[1].equals("p")){
                    //Person Table
                    tempKey = records[0];
                }else{
                    //Email Table
                    v.append(records[0]).append("=>");
                }
            }
            if(tempKey != null && tempKey.trim().length() > 0 && v.toString().trim().length() > 0) {
                k.set(tempKey);
                result.set(v.toString());
                context.write(k, result);
            }
            if(tempKey == null || tempKey.trim().length() == 0){
                k.set("Person");
                result.set(v.toString());
                context.write(k, result);
            }
            if(v.toString().trim().length() == 0){
                result.set("Email");
                k.set(tempKey);
                context.write(k, result);
            }
        }
    }
}
