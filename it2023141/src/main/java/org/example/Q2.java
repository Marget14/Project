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
import java.util.StringTokenizer;

public class Q2 {

    public static class FileAwareMapper
            extends Mapper<Object, Text, Text, Text> {

        private Text word = new Text();
        private Text fileName = new Text();

        @Override
        protected void setup(Context context)
                throws IOException, InterruptedException {
            // Παίρνουμε το όνομα του αρχείου
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
            // Διαβάζουμε μόνο τα δύο αρχεία που μας ενδιαφέρουν
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

    public static class DifferenceReducer
            extends Reducer<Text, Text, Text, NullWritable> {

        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            boolean inPg100 = false;
            boolean inPg24269 = false;

            // Ελέγχουμε σε ποια αρχεία υπάρχει η λέξη
            for (Text file : values) {
                if (file.toString().equals("pg100")) {
                    inPg100 = true;
                } else if (file.toString().equals("pg24269")) {
                    inPg24269 = true;
                }
            }

            // Εκτυπώνουμε μόνο αν υπάρχει στο pg24269 και ΟΧΙ στο pg100
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
        job.setReducerClass(DifferenceReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}