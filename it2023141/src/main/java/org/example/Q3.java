package org.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
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
import java.util.TreeSet;

public class Q3 {

    public static class InvertedIndexMapper
            extends Mapper<Object, Text, Text, Text> {

        private Text word = new Text();
        private Text fileName = new Text();

        @Override
        protected void setup(Context context)
                throws IOException, InterruptedException {
            String filePath = ((FileSplit) context.getInputSplit())
                    .getPath().getName();
            fileName.set(filePath.replace(".txt", ""));
        }

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {

            StringTokenizer itr = new StringTokenizer(value.toString()
                    .toLowerCase().replaceAll("[^a-z\\s]", " "));
            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());
                context.write(word, fileName);
            }
        }
    }
    public static class InvertedIndexCombiner
            extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {

            Set<String> fileSet = new HashSet<>();

            for (Text file : values) {
                fileSet.add(file.toString());
            }

            for (String file : fileSet) {
                context.write(key, new Text(file));
            }
        }
    }

    public static class InvertedIndexReducer
            extends Reducer<Text, Text, Text, Text> {

        private Text result = new Text();

        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {

            Set<String> fileSet = new TreeSet<>();
            StringBuilder fileList = new StringBuilder();

            for (Text file : values) {
                fileSet.add(file.toString());
            }

            boolean first = true;
            for (String file : fileSet) {
                if (!first) {
                    fileList.append(", ");
                }
                fileList.append(file);
                first = false;
            }

            result.set(fileList.toString());
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "inverted index");
        job.setJarByClass(Q3.class);
        job.setMapperClass(InvertedIndexMapper.class);
        job.setCombinerClass(InvertedIndexCombiner.class);
        job.setReducerClass(InvertedIndexReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}