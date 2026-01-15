package org.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

public class Q4 {

    public static class FileCountMapper
            extends Mapper<Object, Text, Text, Text> {

        private Text word = new Text();
        private Text fileTag = new Text();

        @Override
        protected void setup(Context context)
                throws IOException, InterruptedException {
            String fileName = ((FileSplit) context.getInputSplit())
                    .getPath().getName();
            fileTag.set(fileName.replace(".txt", ""));
        }

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {

            StringTokenizer itr = new StringTokenizer(value.toString()
                    .toLowerCase().replaceAll("[^a-z\\s]", " "));
            while (itr.hasMoreTokens()) {
                String token = itr.nextToken();
                // Φιλτράρισμα λέξεων με 5+ χαρακτήρες
                if (token.length() >= 5) {
                    word.set(token + ":" + fileTag.toString());
                    context.write(word, new Text("1"));
                }
            }
        }
    }

    public static class FileCountReducer
            extends Reducer<Text, Text, Text, Text> {

        private Text result = new Text();

        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            int sum = 0;
            for (Text val : values) {
                sum += Integer.parseInt(val.toString());
            }

            String[] parts = key.toString().split(":");
            if (parts.length == 2) {
                String word = parts[0];
                String file = parts[1];
                result.set(file + ":" + sum);
                context.write(new Text(word), result);
            }
        }
    }

    public static class AggregateMapper
            extends Mapper<Object, Text, Text, Text> {

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String[] parts = value.toString().split("\\s+");
            if (parts.length == 2) {
                context.write(new Text(parts[0]), new Text(parts[1]));
            }
        }
    }

    public static class AggregateReducer
            extends Reducer<Text, Text, Text, Text> {

        private Text result = new Text();

        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {

            Map<String, Integer> fileCounts = new HashMap<>();
            fileCounts.put("pg100", 0);
            fileCounts.put("pg46", 0);
            fileCounts.put("pg24269", 0);

            // Συλλέγουμε τα counts για κάθε αρχείο
            for (Text val : values) {
                String[] parts = val.toString().split(":");
                if (parts.length == 2) {
                    String file = parts[0];
                    int count = Integer.parseInt(parts[1]);
                    fileCounts.put(file, count);
                }
            }

            // Ελέγχουμε αν η λέξη εμφανίζεται σε τουλάχιστον 2 αρχεία
            int fileAppearances = 0;
            for (Integer count : fileCounts.values()) {
                if (count > 0) {
                    fileAppearances++;
                }
            }

            if (fileAppearances >= 2) {
                String output = fileCounts.get("pg100") + ", " +
                        fileCounts.get("pg46") + ", " +
                        fileCounts.get("pg24269");
                result.set(output);
                context.write(key, result);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        // Πρώτο MapReduce - word count ανά αρχείο
        Configuration conf1 = new Configuration();
        Job job1 = Job.getInstance(conf1, "word count per file");
        job1.setJarByClass(Q4.class);
        job1.setMapperClass(FileCountMapper.class);
        job1.setReducerClass(FileCountReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job1, new Path(args[0]));
        Path tempPath = new Path("temp_output");
        FileOutputFormat.setOutputPath(job1, tempPath);

        if (job1.waitForCompletion(true)) {
            // Δεύτερο MapReduce - aggregation
            Configuration conf2 = new Configuration();
            Job job2 = Job.getInstance(conf2, "aggregate counts");
            job2.setJarByClass(Q4.class);
            job2.setMapperClass(AggregateMapper.class);
            job2.setReducerClass(AggregateReducer.class);
            job2.setOutputKeyClass(Text.class);
            job2.setOutputValueClass(Text.class);
            FileInputFormat.addInputPath(job2, tempPath);
            FileOutputFormat.setOutputPath(job2, new Path(args[1]));
            System.exit(job2.waitForCompletion(true) ? 0 : 1);
        }
        System.exit(1);
    }
}
