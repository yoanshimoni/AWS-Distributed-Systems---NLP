import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
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
import java.util.Set;

/*As an example, here are the 3,000,000th and 3,000,001st lines from the
 a file of the English 1-grams (googlebooks-eng-all-1gram-20120701-a.gz):
circumvallate   1978   335    91
circumvallate   1979   261    91
The first line tells us that in 1978, the word "circumvallate"
(which means "surround with a rampart or other fortification", in case you were wondering)
occurred 335 times overall, in 91 distinct books of our sample.*/


public class StepOne {
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

    public static String[] arr = {"", "״", "׳", "של", "רב", "פי", "עם", "עליו", "עליהם", "על", "עד", "מן",
            "מכל", "מי", "מהם", "מה", "מ", "למה", "לכל", "לי", "לו", "להיות", "לה",
            "לא", "כן", "כמה", "כלי", "כל", "כי", "יש", "ימים", "יותר", "יד", "י", "זה", "ז", "ועל", "ומי",
            "ולא", "וכן", "וכל", "והיא",
            "והוא", "ואם", "ו", "הרבה", "הנה", "היו", "היה", "היא", "הזה",
            "הוא", "דבר", "ד", "ג", "בני", "בכל", "בו", "בה", "בא", "את", "אשר", "אם", "אלה",
            "אל", "אך", "איש", "אין", "אחת", "אחר", "אחד", "אז", "אותו", "־", "^", "?", ";", ":", "1", ".", "-", "*",
            "!", "שלשה", "בעל", "פני", ")", "גדול", "שם", "עלי", "עולם", "מקום", "לעולם",
            "לנו", "להם", "ישראל", "יודע", "זאת", "השמים", "הזאת", "הדברים", "הדבר",
            "הבית", "האמת", "דברי", "במקום", "בהם", "אמרו", "אינם", "אחרי", "אותם", "אדם", "(", "חלק",
            "שני", "שכל", "שאר", "ש", "ר", "פעמים", "נעשה", "ן", "ממנו", "מלא", "מזה", "ם", "לפי", "ל",
            "כמו", "כבר", "כ", "זו", "ומה", "ולכל", "ובין", "ואין", "הן",
            "היתה", "הא", "ה", "בל", "בין", "בזה", "ב", "אף", "אי", "אותה", "או", "אבל", "א"
    };
    public static final HashSet<String> hebrew_set = new HashSet<>(Arrays.asList(arr));
    public static final Set<String> english_set = new HashSet<>(Arrays.asList(stopWords));


    private static class MyMapper extends Mapper<LongWritable, Text, DoubleGramKey, IntWritable> {

        private static final char FIRST_LOWER_ENG_CHAR = 'a';
        private static final char LAST_LOWER_ENG_CHAR = 'z';
        private static final char FIRST_UPPER_ENG_CHAR = 'A';
        private static final char LAST_UPPER_ENG_CHAR = 'Z';
        private static final char FIRST_HEB_CHAR = (char) 1488;
        private static final char LAST_HEB_CHAR = (char) 1514;

        private static boolean isLetterOrSpace(char c) {

            /*return ((c >= FIRST_LOWER_ENG_CHAR && c <= LAST_LOWER_ENG_CHAR) ||
                    (c >= FIRST_UPPER_ENG_CHAR && c <= LAST_UPPER_ENG_CHAR) ||
                    c == ' ');*/
            return ((c >= FIRST_HEB_CHAR && c <= LAST_HEB_CHAR) || c == ' ');

        }

        private static boolean onlyLettersAndSpace(String string) {
            for (int i = 0; i < string.length(); i++) {
                if (!isLetterOrSpace(string.charAt(i))) {
                    return false;
                }
            }
            return true;
        }

        public void map(LongWritable key, Text val, Context context) throws IOException, InterruptedException {
            String[] line = val.toString().split("\t");
            String[] words = line[0].split(" ");
            String decade_str = line[1].substring(0, line[1].length() - 1);
            decade_str = new StringBuilder().append(decade_str).append("0").toString(); //  the decade. for example 1990
            IntWritable decade = new IntWritable(Integer.parseInt(decade_str));
            IntWritable occurrences = new IntWritable(Integer.parseInt(line[2]));
            for (String word : words) {
                if (english_set.contains(word.toLowerCase()) || hebrew_set.contains(word)) {
                    return;
                }
                // filter only ngrams that contain non-alphabetic characters
               /* if (!onlyLettersAndSpace(word)) {
                    return;
                }*/
            }
            if (words.length == 1) {
                context.write(new DoubleGramKey(new Text(words[0]), new Text("*"), decade), occurrences);
                context.write(new DoubleGramKey(decade), occurrences); //<* * decade, occurs>
            } else if (words.length == 2) {
                context.write(new DoubleGramKey(new Text(words[0]), new Text(words[1]), decade), occurrences);
            }
            // this is how we will cound number of words per decade
            // example *^&decade_couneter 1990 , 20
        }
    }


    private static class MyReducer extends Reducer<DoubleGramKey, IntWritable, Text, Text> {
        private int w1_Occurrences = 0;


        public void reduce(DoubleGramKey key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable value : values) {
                sum += value.get();
            }


            // 1-gram or decade: // <decade w1, c(w1)> or <decade, total(decade)>
            if (key.getWord2().toString().equals("*")) {
                context.write(new Text(key.toString()), new Text("" + sum));

                if (!key.getWord1().toString().equals("*")) { // 1-gram
                    w1_Occurrences = sum;
                }

            }
            // 2-gram: <decade w1 w2, c(<w1,w2>) c(w1)>
            else {
                context.write(
                        new Text(key.toString()),
                        new Text(String.valueOf(sum) + " " + String.valueOf(w1_Occurrences)));
            }
            // value can only be c(w1) or total(decade) or c(<w1,w2>) c(w1)
        }
    }

    private static class MyPartitioner extends Partitioner<DoubleGramKey, IntWritable> {
        // ensure that keys with same word1 are directed to the same reducer
        @Override
        public int getPartition(DoubleGramKey key, IntWritable value, int numPartitions) {
            return Math.abs(key.getWord1().toString().hashCode()) % numPartitions;
        }
    }

    private static class MyCombiner extends Reducer<DoubleGramKey, IntWritable, DoubleGramKey, IntWritable> {


        @Override
        public void reduce(DoubleGramKey key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable value : values) {
                sum += value.get();
            }
            context.write(key, new IntWritable(sum));

        }
    }


    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        System.out.println("starting EMR");

        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);
        job.setJarByClass(StepOne.class);
        job.setMapperClass(MyMapper.class);
        job.setMapOutputKeyClass(DoubleGramKey.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setReducerClass(MyReducer.class);
        job.setCombinerClass(MyCombiner.class);
        job.setNumReduceTasks(1);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setPartitionerClass(StepOne.MyPartitioner.class);
        //TODO change the input format
        //for aws run with a gram
//        job.setInputFormatClass(TextInputFormat.class);
//        SequenceFileInputFormat.addInputPath(job, new Path(args[0]));
//        SequenceFileInputFormat.addInputPath(job, new Path(args[1]));
//        //for pc run
        job.setInputFormatClass(TextInputFormat.class);
        SequenceFileInputFormat.addInputPath(job, new Path("/home/maor/Desktop/DSP202/ass2_202/googlebooks-eng-all-1gram-20120701-z"));
        SequenceFileInputFormat.addInputPath(job, new Path("/home/maor/Desktop/DSP202/ass2_202/googlebooks-eng-all-2gram-20120701-zy"));
        String output = "output";
        // for complete aws ngmram run
//        job.setInputFormatClass(SequenceFileInputFormat.class);
//        FileInputFormat.setInputPaths(job, new Path(args[0]));
//        FileInputFormat.setInputPaths(job, new Path(args[1]));
//        String output = args[2];
        //TODO change the output for args[2] in aws run
        job.setOutputFormatClass(TextOutputFormat.class);
        FileOutputFormat.setOutputPath(job, new Path(output));
        job.waitForCompletion(true);
    }
}
