import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;

/*As an example, here are the 3,000,000th and 3,000,001st lines from the
 a file of the English 1-grams (googlebooks-eng-all-1gram-20120701-a.gz):
circumvallate   1978   335    91
circumvallate   1979   261    91
The first line tells us that in 1978, the word "circumvallate"
(which means "surround with a rampart or other fortification", in case you were wondering)
occurred 335 times overall, in 91 distinct books of our sample.*/

public class StepOne {
    private static class MyMapper extends Mapper<LongWritable, Text, Text, Text> {

        public void map(LongWritable key, Text val, Context context) throws IOException, InterruptedException {
            String[] line = val.toString().split("\t");
            String decade = line[1].substring(0, line[1].length() - 1);
            decade = new StringBuilder().append(decade).append("0").toString();
            int occurs = Integer.parseInt(line[2]);
            Text occurs_Value = new Text(), decade_counter_Key = new Text();
            occurs_Value.set(String.format("%d", occurs));
            decade_counter_Key.set(String.format("*^&decade_couneter %s", decade));
//            context.write(ngram_and_year_Key, occurs_Value);
            context.write(decade_counter_Key, occurs_Value);

        }
    }

    private static class MyReducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            System.out.println("key is\t" + key);
            String w1 = key.toString();
            Text newKey = new Text(), newValue = new Text();
            long totalOccurs = 0L;
            for (Text value : values) {
                totalOccurs += Long.parseLong(value.toString());
                System.out.println("value is \t" + value);
            }

            newKey.set(String.format("%s", w1));
            newValue.set(String.format("%d", totalOccurs));
            System.out.printf("final result is\n%s : %d\n", w1, totalOccurs);
            context.write(newKey, newValue);

        }
    }

    private static class MyPartitioner extends Partitioner<Text, Text> {
        @Override
        public int getPartition(Text key, Text value, int numPartitions) {
            return Math.abs(key.hashCode()) % numPartitions;
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        System.out.println("starting EMR");

        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);
        job.setJarByClass(StepOne.class);
        job.setMapperClass(MyMapper.class);
        job.setCombinerClass(MyReducer.class);
        job.setReducerClass(MyReducer.class);
        job.setNumReduceTasks(1);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setPartitionerClass(StepOne.MyPartitioner.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        //TODO change the input format
        job.setInputFormatClass(SequenceFileInputFormat.class);
        SequenceFileInputFormat.addInputPath(job, new Path(args[0]));
        job.setInputFormatClass(TextInputFormat.class);
//        FileInputFormat.setInputPaths(job, new Path(args[0]));
        String output = args[1];
        FileOutputFormat.setOutputPath(job, new Path(output));
        job.waitForCompletion(true);
    }
}
