import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

//s3://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/3gram/data


import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class step1_count_N_and_split_corpus {
    public static Counter counter;




    public static class MapperClass extends Mapper<LongWritable, Text, Text, Text> {
//        private static Pattern pattern = Pattern.compile("[א-ת]+");
        Pattern pattern;
        Pattern pattern_num;
//        alphabetically comes before any char
//        Text big_counter_N = new Text("#");
        Text big_counter_N;

        @Override
        protected void setup(Mapper<LongWritable, Text, Text, Text>.Context context) {
            pattern = Pattern.compile("[א-ת]+");
            pattern_num = Pattern.compile("[0-9]+");
            big_counter_N = new Text("#");
//            count = 0;
//            dont_care = new Date().getTime();
        }

        @Override
        public void map(LongWritable lineId, Text line, Context context) throws IOException,  InterruptedException {
            Text val;
            String[] line_as_string = line.toString().split("\\s+");
            Text key ;
            System.out.println("Map::::" + lineId.toString() + ". " + line.toString() + "");
            if (line_as_string.length > 4) {

                Matcher m1 = pattern.matcher(line_as_string[0]);
                Matcher m2 = pattern.matcher(line_as_string[1]);
                Matcher m3 = pattern.matcher(line_as_string[2]);
                boolean w1_len = line_as_string[0].length()>1;
                boolean w2_len = line_as_string[1].length()>1;
                boolean w3_len = line_as_string[2].length()>1;

                boolean b = m1.matches() && m2.matches() && m3.matches() && w1_len && w2_len && w3_len;
                if (b) {
                    String occurrences = line_as_string[4];
                    key = new Text(line_as_string[0] + " " + line_as_string[1] + " " + line_as_string[2]);
                    Text oc = new Text(occurrences);
                    if (lineId.get() % 2 == 0) {
//                    val = new ArrayWritable(new String[]{occurrences, "0"});

                        val = new Text(occurrences + " 0");
                    } else {
//                    val = new ArrayWritable(new String[]{"0", occurrences});
//                    val = new ArrayWritable(LongWritable.class,new Writable[]{oc,new Text("0")});
                        val = new Text("0 " + occurrences);
                    }
                    context.write(big_counter_N, oc);
                    context.write(key, val);
                }
            }
        }

    }

    public static class ReducerClass extends Reducer<Text,Text,Text,Text> {


        @Override
        protected void setup(Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
             counter = context.getCounter(all_steps.NCounter.N_COUNTER);
        }

        @Override
        protected void reduce(Text key, Iterable<Text> values, Reducer<Text,Text,Text,Text>.Context context) throws IOException, InterruptedException {
            long sum_of_first_corpus = 0;
            long sum_of_second_corpus = 0;
            if(key.toString().equals("#")){
                long big_counter_N = 0;
                for (Text value : values) {

                    long add_to_counter;
                    try {
                        add_to_counter = Long.parseLong(value.toString());
                    }
                    catch (NumberFormatException e){
                        add_to_counter = 0;
                        System.out.println("NumberFormatException " + value.toString());
                    }
                    big_counter_N += add_to_counter;


                }
                long old_counter = counter.getValue();

                counter.setValue(old_counter + big_counter_N);
                counter = context.getCounter(all_steps.NCounter.N_COUNTER);
                old_counter = counter.getValue();
                System.out.println("===================================================" + old_counter);


            }
            else {
                for (Text value : values) {
                    String [] occ_in_each_corpus = value.toString().split("\\s+");
                    try {
                        sum_of_first_corpus += Long.parseLong(occ_in_each_corpus[0]);
                    }
                    catch (NumberFormatException e){
                        System.out.println("NumberFormatException " + value.toString());
                        System.out.println("occ_in_each_corpus[0] " + occ_in_each_corpus[0]);
                        System.out.println("occ_in_each_corpus[1] " + occ_in_each_corpus[1]);

                    }
                    try {
                        sum_of_second_corpus += Long.parseLong(occ_in_each_corpus[1]);
                    }
                    catch (NumberFormatException e){
                    }
                }
//            String [] corpuses = new String[] {String.valueOf(sum_of_first_corpus),String.valueOf(sum_of_second_corpus)};
                String corpuses = sum_of_first_corpus + " " + sum_of_second_corpus;
//            String [] corpuses = new String[] {"1","2"};
//            String corpuses = "1"+"2";
                context.write(key, new Text(corpuses));
            }
        }

//        @Override
//        protected void cleanup(Reducer<Text,ArrayWritable,Text,ArrayWritable>.Context context) throws IOException, InterruptedException {
//        }

    }
    public static class CombinerClass extends Reducer<Text,Text,Text,Text> {


        @Override
        protected void reduce(Text key, Iterable<Text> values, Reducer<Text,Text,Text,Text>.Context context) throws IOException, InterruptedException {
            long sum_of_first_corpus = 0;
            long sum_of_second_corpus = 0;
            if(key.toString().equals("#")){
                long big_counter_N = 0;
                for (Text value : values) {

                    long add_to_counter;
                    try {
                        add_to_counter = Long.parseLong(value.toString());
                    }
                    catch (NumberFormatException e){
                        add_to_counter = 0;
                        System.out.println("NumberFormatException " + value.toString());
                    }
                    big_counter_N += add_to_counter;


                }
                String combined_counter = Long.toString(big_counter_N);

                context.write(key,new Text(combined_counter));


            }
            else {
                for (Text value : values) {
                    String [] occ_in_each_corpus = value.toString().split("\\s+");
                    try {
                        sum_of_first_corpus += Long.parseLong(occ_in_each_corpus[0]);
                    }
                    catch (NumberFormatException e){
                        System.out.println("NumberFormatException " + value.toString());
                        System.out.println("occ_in_each_corpus[0] " + occ_in_each_corpus[0]);
                        System.out.println("occ_in_each_corpus[1] " + occ_in_each_corpus[1]);

                    }
                    try {
                        sum_of_second_corpus += Long.parseLong(occ_in_each_corpus[1]);
                    }
                    catch (NumberFormatException e){
                    }
                }
                String corpuses = sum_of_first_corpus + " " + sum_of_second_corpus;
                context.write(key, new Text(corpuses));
            }
        }

//        @Override
//        protected void cleanup(Reducer<Text,ArrayWritable,Text,ArrayWritable>.Context context) throws IOException, InterruptedException {
//        }

    }


//    public static void main(String[] args) throws Exception {
//        Configuration conf = new Configuration();
//        Job job = Job.getInstance(conf, "step1_count_N_and_split_corpus");
//        job.setJarByClass(step1_count_N_and_split_corpus.class);
//        job.setMapperClass(step1_count_N_and_split_corpus.MapperClass.class);
//        job.setReducerClass(step1_count_N_and_split_corpus.ReducerClass.class);
//        job.setMapOutputKeyClass(Text.class);
//        job.setMapOutputValueClass(Text.class);
//        job.setOutputKeyClass(Text.class);
//        job.setOutputValueClass(Text.class);
//        FileInputFormat.addInputPath(job, new Path(args[0]));
//        FileOutputFormat.setOutputPath(job, new Path(args[1]));
//        job.setInputFormatClass(TextInputFormat.class);
//        job.setOutputFormatClass(TextOutputFormat.class);
//        System.exit(job.waitForCompletion(true) ? 0 : 1);
//    }

}