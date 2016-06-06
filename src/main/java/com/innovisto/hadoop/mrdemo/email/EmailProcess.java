package com.innovisto.hadoop.mrdemo.email;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

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
            String[] tokens = line.split("\\t");
            String tableName = ((FileSplit) context.getInputSplit()).getPath().getName();
            if(tableName.equals("name.txt")){
                //1th
                k.set(tokens[0]);
                v.set(tokens[1]+"^n");
            }else {
                //5th
                k.set(tokens[0]);
                v.set(tokens[1]+"^p");
            }
            context.write(k,v);
        }

    }
    public static class ReducingTask extends Reducer<Text,Text,Text,Text> {
        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            List<String> keyList = new ArrayList<>();
            List<String> valueList = new ArrayList<>();
            for (Text val : values) {
                String record = val.toString();
                valueList.add(record);
                context.setStatus(key.toString()+" : "+record);
                String[] records = record.split("^");
                if(records.length != 2) continue;
                if(records[1].equals("n")){
                    //Person Table
                    keyList.add(records[0]);
                }else{
                    //Email Table
                    valueList.add(records[0]);
                }
            }
            Text vT = new Text();
            Text kT = new Text();
            StringBuilder builder = new StringBuilder();
            for(String v : valueList){
                if(v != null){
                    builder.append(v);
                }
            }
            vT.set(builder.toString());
            kT.set("dipak");
            context.write(kT, vT);
        }
    }
}
