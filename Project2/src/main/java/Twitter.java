import java.io.*;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Scanner;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;


public class Twitter {

    public static void main(String[] args) throws Exception {

        // First MapReduce Job
        Job job1 = Job.getInstance();
        job1.setJobName("MapperOneJob");
        job1.setJarByClass(Twitter.class);
        job1.setOutputKeyClass(IntWritable.class);
        job1.setOutputValueClass(IntWritable.class);
        job1.setMapOutputKeyClass(IntWritable.class);
        job1.setMapOutputValueClass(IntWritable.class);
        job1.setMapperClass(MapperOne.class);
        job1.setCombinerClass(CombinerOne.class);
        job1.setReducerClass(ReducerOne.class);
        job1.setInputFormatClass(TextInputFormat.class);
        job1.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.setInputPaths(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1]));
        job1.waitForCompletion(true);

        // Second MapReduce Job
        Job job2 = Job.getInstance();
        job2.setJobName("MapperTwoJob");
        job2.setJarByClass(Twitter.class);
        job2.setOutputKeyClass(IntWritable.class);
        job2.setOutputValueClass(IntWritable.class);
        job2.setMapOutputKeyClass(IntWritable.class);
        job2.setMapOutputValueClass(IntWritable.class);
        job2.setMapperClass(MapperTwo.class);
        job2.setReducerClass(ReducerTwo.class);
        job2.setInputFormatClass(TextInputFormat.class);
        job2.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.setInputPaths(job2, new Path(args[1]));
        FileOutputFormat.setOutputPath(job2, new Path(args[2]));
        job2.waitForCompletion(true);
    }

    public static class MapperOne extends Mapper<Object, Text, IntWritable, IntWritable> {
        @Override
        public void map(Object key, Text value, Context ct)
                throws IOException, InterruptedException {
            Scanner scanFile = new Scanner(value.toString()).useDelimiter(",");
            int id = scanFile.nextInt();
            int follower_id = scanFile.nextInt();
            ct.write(new IntWritable(follower_id), new IntWritable(id));
            scanFile.close();
        }
    }

    public static class CombinerOne extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
        @Override
        public void reduce(IntWritable key, Iterable<IntWritable> values, Context ct)
                throws IOException, InterruptedException {
            int count = 0;
            for (IntWritable value: values){
                count++;
            }
            ct.write(key, new IntWritable(count));
        }
    }

    public static class ReducerOne extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
        @Override
        public void reduce(IntWritable key, Iterable<IntWritable> values, Context ct)
                throws IOException, InterruptedException {
            int count = 0;
            for (IntWritable value: values){
                count = count + value.get();
            }
            ct.write(key, new IntWritable(count));
        }
    }

    public static class MapperTwo extends Mapper<Object, Text, IntWritable, IntWritable> {

        HashMap<Integer, Integer> count2;

        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            count2 = new HashMap<Integer, Integer>();
        }

        @Override
        public void map(Object key, Text value, Context ct)
                throws IOException, InterruptedException {
            Scanner scanFile = new Scanner(value.toString()).useDelimiter("\t");
            int follower_id = scanFile.nextInt();
            int count = scanFile.nextInt();
            if (count2.get(count) == null){
                count2.put(count, 1);
            }
            else{
                int xx = count2.get(count) + 1;
                count2.put(count, xx);
            }
            // ct.write(new IntWritable(count), new IntWritable(1));
            scanFile.close();
        }

        @Override
        public void cleanup(Context context) throws IOException, InterruptedException {
            Iterator<HashMap.Entry<Integer, Integer>> temp = count2.entrySet().iterator();
            Integer countVal = 0;
            Integer key1 = 0;
            while(temp.hasNext()) {
                HashMap.Entry<Integer, Integer> entry = temp.next();
                key1 = entry.getKey();
                countVal = entry.getValue();
                context.write(new IntWritable(key1), new IntWritable(countVal));
            }
        }
    }

    public static class ReducerTwo extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
        @Override
        public void reduce(IntWritable key, Iterable<IntWritable> values, Context ct)
                throws IOException, InterruptedException {
            int count = 0;
            for (IntWritable value :
                    values) {
                count = count + value.get();
            }
            ct.write(key, new IntWritable(count));
        }
    }
}
