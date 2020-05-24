import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class StepTwo {


    private static class MyMapper extends Mapper<Text, Text, BigramKey, NGramValue> {
        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            // key: <decade w1 w2>
            // value: occurrences
            // 1-gram or decade: // <decade w1, c(w1)> or <decade, total(decade)>
            // 2-gram: <decade w1 w2, c(<w1,w2>) c(w1)>
            String[] valueWords = value.toString().split(" ");
            String[] keyWords = key.toString().split(" "); //[0:decade 1:w1 2:w2]

            IntWritable decade = new IntWritable(Integer.parseInt(keyWords[0]));
            BigramKey key1;
            NGramValue newVal;
            if (keyWords[2].equals("*")) { // total for decade OR occurrences for 1-gram
                newVal = new NGramValue(new IntWritable(Integer.parseInt(valueWords[0])));
                key1 = new BigramKey(new Text(keyWords[1]), new Text(keyWords[2]), new Text(keyWords[1]), decade);
                // newVal = (occurs)
                // bigramKey = (w1,w2,w1);
            } else { // occurrences for 2-gram
                newVal = new NGramValue(
                        new IntWritable(Integer.parseInt(valueWords[0])),
                        new IntWritable(Integer.parseInt(valueWords[1])));
                key1 = new BigramKey(new Text(keyWords[1]), new Text(keyWords[2]), new Text(keyWords[2]), decade);
                // newVal (c(<w1,w2>) c(w1))
                // bigramKey = (w1,w2,w2);
            }
            context.write(key1, newVal);
            System.out.printf("wrote <%s : %s>\n", key1.toString(), newVal.toString());
        }
    }

    private static class MyReducer extends Reducer<BigramKey, NGramValue, Text, DoubleWritable> {

        private int decadeTotal = 0;
        private int occurrencesW2 = 0;

        @Override
        public void reduce(BigramKey key, Iterable<NGramValue> values, Context context)
                throws IOException, InterruptedException {

            int sum = 0;
            int occurrencesW1 = 0;
            String w1 = key.getWord1().toString();
            String w2 = key.getWord2().toString();

            for (NGramValue value : values) {
                sum += value.getOccurrences().get();
                if (!w1.equals("*") && !w2.equals("*")) //2gram
                    occurrencesW1 += value.getOccurrencesOfW1().get();
                System.out.printf("%s\n",key.toString());
                System.out.printf("value %s\n", value.toString());
            }

            if (w1.equals("*")) // decade
                decadeTotal = sum;
            else if (w2.equals("*")) // 1-gram
                occurrencesW2 = sum;
            else { // 2-gram
                // write the likelihood to the context: <decade likelihood w1 w2>, likelihood>
                DoubleWritable calculation = calculateLikelihood(
                        sum, occurrencesW1, occurrencesW2, decadeTotal);
                context.write(new Text(key.getDecade() + " " + calculation + " " + w1 + " " + w2), calculation);
                System.out.printf("%s\n and calc is %s\n",
                        key.getDecade() + " " + calculation + " " + w1 + " " + w2,
                        calculation.toString() );
            }
        }

        private DoubleWritable calculateLikelihood(double c12, double c1, double c2, double N) {
            double L1 = c12 * Math.log10(c2 / N) + (c1 - c12) * Math.log10(1 - (c2 / N));
            double L2 = (c2 - c12) * Math.log10(c2 / N) + (N + c12 - c1 - c2) * Math.log10(1 - (c2 / N));
            double L3 = c12 * Math.log10(c12 / c1) + (c1 - c12) * Math.log10(1 - (c12 / c1));
            double L4 = (c2 - c12) * Math.log10((c2 - c12) / (N - c1)) +
                    (N + c12 - c1 - c2) * Math.log10(1 - ((c2 - c12) / (N - c1)));

            double formula = L1 + L2 - L3 - L4;
            return new DoubleWritable((-2) * formula);
        }
    }

    private static class MyPartitioner extends Partitioner<BigramKey, NGramValue> {
        // ensure that keys with same decade are directed to the same reducer
        @Override
        public int getPartition(BigramKey key, NGramValue value, int numPartitions) {
            return Math.abs(key.getDecade().toString().hashCode()) % numPartitions;
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);
        job.setJarByClass(StepTwo.class);
        job.setMapperClass(MyMapper.class);
        job.setMapOutputKeyClass(BigramKey.class);
        job.setMapOutputValueClass(NGramValue.class);
        job.setReducerClass(MyReducer.class);
        job.setPartitionerClass(MyPartitioner.class);
//        job.setOutputKeyClass(Text.class);


//        job.setOutputValueClass(Text.class);


//        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setInputFormatClass(KeyValueTextInputFormat.class);
        FileInputFormat.addInputPath(job, new Path("/home/maor/Desktop/DSP202/ass2_202/output/part-r-00000"));

//        SequenceFileInputFormat.addInputPath(job, new Path(""));
//        job.setInputFormatClass(TextInputFormat.class);
//        FileInputFormat.setInputPaths(job, new Path(args[0]));

//        String output = args[1];
        String output = "output_step2";
        job.setOutputFormatClass(TextOutputFormat.class);
        FileOutputFormat.setOutputPath(job, new Path(output));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
