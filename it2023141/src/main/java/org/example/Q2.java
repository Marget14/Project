package org.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;

public class Q2 {

    public static class FileAwareMapper
            extends Mapper<Object, Text, Text, Text> {

        private Text word = new Text();
        private Text fileName = new Text();

        @Override
        protected void setup(Context context)
                throws IOException, InterruptedException {
            String filePath = ((FileSplit) context.getInputSplit())
                    .getPath().toString();
            if (filePath.contains("pg100.txt")) {
                fileName.set("pg100");
            } else if (filePath.contains("pg24269.txt")) {
                fileName.set("pg24269");
            } else {
                fileName.set("other");
            }
        }

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            if (fileName.toString().equals("pg100") ||
                    fileName.toString().equals("pg24269")) {

                StringTokenizer itr = new StringTokenizer(value.toString()
                        .toLowerCase().replaceAll("[^a-z\\s]", " "));
                while (itr.hasMoreTokens()) {
                    word.set(itr.nextToken());
                    context.write(word, fileName);
                }
            }
        }
    }
    public static class FileCombiner
            extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            Set<String> files = new HashSet<>();

            for (Text file : values) {
                files.add(file.toString());
                if (files.size() == 2) {
                    break;
                }
            }

            for (String file : files) {
                context.write(key, new Text(file));
            }
        }
    }

    public static class DifferenceReducer
            extends Reducer<Text, Text, Text, NullWritable> {

        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            boolean inPg100 = false;
            boolean inPg24269 = false;

            for (Text file : values) {
                String fileName = file.toString();
                if (fileName.equals("pg100")) {
                    inPg100 = true;
                } else if (fileName.equals("pg24269")) {
                    inPg24269 = true;
                }

                if (inPg100) {
                    return;
                }

                if (inPg100 && inPg24269) {
                    return;
                }
            }

            if (inPg24269 && !inPg100) {
                context.write(key, NullWritable.get());
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "words in pg24269 not in pg100");
        job.setJarByClass(Q2.class);
        job.setMapperClass(FileAwareMapper.class);
        job.setCombinerClass(FileCombiner.class);
        job.setReducerClass(DifferenceReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}