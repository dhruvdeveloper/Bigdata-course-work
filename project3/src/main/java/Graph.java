import java.io.*;
import java.util.Scanner;
import java.util.Vector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;


class Tagged implements Writable {
    public boolean tag;                // true for a graph vertex, false for distance
    public int distance;               // the distance from the starting vertex
    public Vector<Long> following;  // the vertex neighbors

    public Tagged() {
        tag = false;
    }

    public Tagged(int d) {
        tag = false;
        distance = d;
    }

    public Tagged(int d, Vector<Long> f) {
        tag = true;
        distance = d;
        following = f;
    }

    public void write(DataOutput out) throws IOException {
        out.writeBoolean(tag);
        out.writeLong(distance);
        if (tag) {
            out.writeInt(following.size());
            for (int i = 0; i < following.size(); i++)
                out.writeLong(following.get(i));
        }
    }

    public void readFields(DataInput in) throws IOException {
        tag = in.readBoolean();
        distance = in.readInt();
        if (tag) {
            int n = in.readInt();
            following = new Vector<Long>(n);
            for (int i = 0; i < n; i++)
                following.add(in.readLong());
        }
    }
}

public class Graph {
    static public int start_id = 14701391;
    static public int max_int = Integer.MAX_VALUE;

    /* ... */

    public static void main(String[] args) throws Exception {
        int iterations = 5;
        Job job1 = Job.getInstance();
        job1.setJobName("Job1");
        job1.setJarByClass(Graph.class);
        job1.setMapperClass(MapperOne.class);
        job1.setReducerClass(ReducerOne.class);
        job1.setMapOutputKeyClass(LongWritable.class);
        job1.setMapOutputValueClass(LongWritable.class);
        job1.setOutputKeyClass(LongWritable.class);
        job1.setOutputValueClass(Tagged.class);
        job1.setInputFormatClass(TextInputFormat.class);
        job1.setOutputFormatClass(SequenceFileOutputFormat.class);
        FileInputFormat.setInputPaths(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1] + "0"));
        job1.waitForCompletion(true);
        for (short i = 0; i < iterations; i++) {
            Job job2 = Job.getInstance();
            job2.setJarByClass(Graph.class);
            job2.setMapperClass(MapperTwo.class);
            job2.setReducerClass(ReducerTwo.class);
            job2.setMapOutputKeyClass(LongWritable.class);
            job2.setMapOutputValueClass(Tagged.class);
            job2.setOutputKeyClass(LongWritable.class);
            job2.setOutputValueClass(Tagged.class);
            job2.setInputFormatClass(SequenceFileInputFormat.class);
            job2.setOutputFormatClass(SequenceFileOutputFormat.class);
            FileInputFormat.setInputPaths(job2, new Path(args[1] + i));
            FileOutputFormat.setOutputPath(job2, new Path(args[1] + (i + 1)));
            job2.waitForCompletion(true);
        }
        Job job3 = Job.getInstance();
        /* ... Final Map-Reduce job to calculate the cluster sizes */
        job3.setJarByClass(Graph.class);
        job3.setMapperClass(MapperThree.class);
        job3.setMapOutputKeyClass(LongWritable.class);
        job3.setMapOutputValueClass(LongWritable.class);
        job3.setOutputKeyClass(LongWritable.class);
        job3.setOutputValueClass(LongWritable.class);
        job3.setInputFormatClass(SequenceFileInputFormat.class);
        job3.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.setInputPaths(job3, new Path(args[1] + iterations));
        FileOutputFormat.setOutputPath(job3, new Path(args[2]));
        job3.waitForCompletion(true);
    }

    public static class MapperOne extends Mapper<Object, Text, LongWritable, LongWritable> {
        @Override
        public void map(Object key, Text value, Context ct)
                throws IOException, InterruptedException {
            Scanner scanFile = new Scanner(value.toString()).useDelimiter(",");
            int id = scanFile.nextInt();
            int follower_id = scanFile.nextInt();
            ct.write(new LongWritable(follower_id), new LongWritable(id));
            scanFile.close();
        }
    }

    public static class ReducerOne extends Reducer<LongWritable, LongWritable, LongWritable, Tagged> {
        @Override
        public void reduce(LongWritable key, Iterable<LongWritable> values, Context ct)
                throws IOException, InterruptedException {
            Vector<Long> follow = new Vector<>();
            for (LongWritable value : values) {
                follow.add(value.get());
            }
            if (key.get() == start_id || key.get() == 1) {
                ct.write(key, new Tagged(0, follow));
            } else {
                ct.write(key, new Tagged(max_int, follow));
            }
        }
    }

    public static class MapperTwo extends Mapper<LongWritable, Tagged, LongWritable, Tagged> {
        public void map(LongWritable key, Tagged value, Context ct)
                throws IOException, InterruptedException {
            ct.write(key, value);
            if (value.distance < max_int) {
                for (long n : value.following) {
//                    ct.write(new LongWritable(n), new Tagged(value.distance + 1));
                    value.distance = value.distance + 1;
                    Tagged t = new Tagged(value.distance);
                    ct.write(new LongWritable(n), t);
                }
            }
        }
    }

    public static class ReducerTwo extends Reducer<LongWritable, Tagged, LongWritable, Tagged> {
        public void reduce(LongWritable key, Iterable<Tagged> values, Context ct)
                throws IOException, InterruptedException {
            int m = max_int;
            Vector<Long> following = null;
            for (Tagged t : values) {
                if (t.distance < m) {
                    m = t.distance;
                }
                if (t.tag) {
                    following = t.following;
                }
            }
            ct.write(key, new Tagged(m, following));
        }
    }

    public static class MapperThree extends Mapper<LongWritable, Tagged, LongWritable, LongWritable> {
        public void map(LongWritable key, Tagged value, Context ct)
                throws IOException, InterruptedException {
            if (value.distance < max_int) {
                ct.write(key, new LongWritable(value.distance));
            }
        }
    }
}
