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
        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            String v = "";
            String tempKey = "";
            for (Text val : values) {
                String record = val.toString();
                String[] records = record.split("^");
                if(records.length != 2) continue;
                for(Text valNext : values){
                    String recordNext = valNext.toString();
                    String[] recordNexts = recordNext.split("^");
                    if(recordNexts.length != 2) continue;
                    if(!records[1].equals(recordNexts[1])){
                        tempKey = records[0]+" => "+recordNexts[0];
                    }
                }
               /* String[] s = val.toString().split("^");
                if(s.length == 2) {
                    if (s[1].equals("p")) {
                        tempKey = s[0];
                    } else if(s[1].equals("e")){
                        v += s[0] + ", ";
                    }
                }
                k.set(tempKey);
                result.set(v);
                context.write(k, result);*/
            }
            k.set("a");
            result.set(tempKey);
            context.write(k, result);
        }
    }
}
