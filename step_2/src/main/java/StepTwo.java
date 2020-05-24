import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class StepTwo {
    public static String[] stopWords = {"a",
            "about", "above", "across", "after", "afterwards", "again", "against", "all", "almost",
            "alone", "along", "already", "also", "although", "always", "am", "among", "amongst",
            "amoungst", "amount", "an", "and", "another", "any", "anyhow", "anyone", "anything",
            "anyway", "anywhere", "are", "around", "as", "at", "back", "be", "became", "because",
            "become", "becomes", "becoming", "been", "before", "beforehand", "behind", "being",
            "below", "beside", "besides", "between", "beyond", "bill", "both", "bottom", "but",
            "by", "call", "can", "cannot", "cant", "co", "computer", "con", "could", "couldn't",
            "cry", "de", "describe", "detail", "do", "done", "down", "due", "during", "each",
            "eg", "eight", "either", "eleven", "else", "elsewhere", "empty", "enough", "etc",
            "even", "ever", "every", "everyone", "everything", "everywhere", "except", "few",
            "fifteen", "fify", "fill", "find", "fire", "first", "five", "for", "former",
            "formerly", "forty", "found", "four", "from", "front", "full", "further", "get",
            "give", "go", "had", "has", "hasn't", "have", "he", "hence", "her", "here", "hereafter",
            "hereby", "herein", "hereupon", "hers", "herself", "him", "himself", "his", "how", "however",
            "hundred", "i", "ie", "if", "in", "inc", "indeed", "interest", "into", "is", "it", "its",
            "itself", "keep", "last", "latter", "latterly", "least", "less", "ltd", "made", "many", "may",
            "me", "meanwhile", "might", "mill", "mine", "more", "moreover", "most", "mostly", "move",
            "much", "must", "my", "myself", "name", "namely", "neither", "never", "nevertheless",
            "next", "nine", "no", "nobody", "none", "noone", "nor", "not", "nothing", "now",
            "nowhere", "of", "off", "often", "on", "once", "one", "only", "onto", "or", "other",
            "others", "otherwise", "our", "ours", "ourselves", "out", "over", "own", "part",
            "per", "perhaps", "please", "put", "rather", "re", "same", "see", "seem", "seemed",
            "seeming", "seems", "serious", "several", "she", "should", "show", "side", "since",
            "sincere", "six", "sixty", "so", "some", "somehow", "someone", "something", "sometime",
            "sometimes", "somewhere", "still", "such", "system", "take", "ten", "than", "that",
            "the", "their", "them", "themselves", "then", "thence", "there", "thereafter",
            "thereby", "therefore", "therein", "thereupon", "these", "they", "thick", "thin",
            "third", "this", "those", "though", "three", "through", "throughout", "thru",
            "thus", "to", "together", "too", "top", "toward", "towards", "twelve", "twenty",
            "two", "un", "under", "until", "up", "upon", "us", "very", "via", "was", "we", "well",
            "were", "what", "whatever", "when", "whence", "whenever", "where", "whereafter",
            "whereas", "whereby", "wherein", "whereupon", "wherever", "whether", "which", "while",
            "whither", "who", "whoever", "whole", "whom", "whose", "why", "will", "with", "within",
            "without", "would", "yet", "you", "your", "yours", "yourself", "yourselves"};
    public static final Set<String> stopSet = new HashSet<>(Arrays.asList(stopWords));

    private static class MyMapper extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable key, Text val, Context context) throws IOException, InterruptedException {

            String[] lines = val.toString().split("\t"),
                    words = lines[0].split(" ");

            if (words.length > 1) {

                String w1 = words[0], w2 = words[1];
                Text text = new Text(), text1 = new Text();
                int occurs = Integer.parseInt(lines[2]);

                text.set(String.format("%s %s", w1, w2));
                text1.set(String.format("%d", occurs));

                context.write(text, text1);

            }
        }
    }

    private static class MyReducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String w1 = key.toString();
            Text newKey = new Text(), newValue = new Text();
            long totalOccurs = 0L;

            for (Text value : values) {
                totalOccurs += Long.parseLong(value.toString());
            }

            newKey.set(String.format("%s", w1));
            newValue.set(String.format("%d", totalOccurs));

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
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);
        job.setJarByClass(StepTwo.class);
        job.setMapperClass(StepTwo.MyMapper.class);
        job.setCombinerClass(StepTwo.MyReducer.class);
        job.setReducerClass(StepTwo.MyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setPartitionerClass(StepTwo.MyPartitioner.class);
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
