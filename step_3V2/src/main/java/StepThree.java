import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class StepThree {

    public static class MyMapper extends Mapper<Text, DoubleWritable, SortingKey, Text> {

        @Override
        public void map(Text key, DoubleWritable value, Context context) throws IOException, InterruptedException {

            // split the key to 4 parts
            String[] words = key.toString().split(" ");
            String decade = words[0];
            String likelihood = words[1];
            String w1 = words[2];
            String w2 = words[3];

            SortingKey sortingKey = new SortingKey(decade, w1, w2, value.get());

            // Send to context for sorting
            context.write(sortingKey, new Text(likelihood));
        }
    }

    public static class MyReducer extends Reducer<SortingKey, Text, Text, DoubleWritable> {
        private int counter;
        private String decade = "";

        @Override
        public void reduce(SortingKey key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            if (!decade.equals(key.getDecade())) {
                decade = key.getDecade();
                counter = 0;
            }

            if (counter < 100) {
                counter++;
                context.write(
                        new Text(counter + ". " + key.getWords()),
                        new DoubleWritable(key.getLikelihood()));
            }
        }
    }

    public static class MyPartition extends Partitioner<SortingKey, Text> {

        // ensure that keys with same decade are directed to the same reducer
        @Override
        public int getPartition(SortingKey key, Text value, int numPartitions) {
            return Math.abs(key.getDecade().hashCode()) % numPartitions;
        }
    }


    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);
        job.setJarByClass(StepThree.class);
        // set mapper and reducer
        job.setMapperClass(MyMapper.class);
        job.setMapOutputKeyClass(SortingKey.class);
        job.setMapOutputValueClass(Text.class);

        job.setReducerClass(MyReducer.class);

        // set partitioner
        job.setPartitionerClass(MyPartition.class);

        // set input
        job.setInputFormatClass(TextDoubleInputFormat.class);
        FileInputFormat.addInputPath(job, new Path("/home/maor/Desktop/DSP202/ass2_202/output_step2/part-r-00000"));

        String output = "output_step3";

        job.setOutputFormatClass(TextOutputFormat.class);
        FileOutputFormat.setOutputPath(job, new Path(output));

        // wait for completion and exit
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
