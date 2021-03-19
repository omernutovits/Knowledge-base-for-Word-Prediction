import java.io.IOException;

//s3://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/3gram/data

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class step3_calculate_probability {



    public static class Mapper_pass_to_reducer extends Mapper<LongWritable, Text, Text, Text> {
//        LongWritable one;

        @Override
        protected void setup(Mapper<LongWritable, Text, Text, Text>.Context context) {

//            one = new LongWritable(1);
        }

        @Override
        public void map(LongWritable lineId, Text line, Context context) throws IOException,  InterruptedException {
            String[] line_as_string = line.toString().split("\\s+");

            System.out.println("Map::Mapper_Nr_Tr_input::" + lineId.toString() + ". " + line.toString() + "");
            if (line_as_string.length > 3) {
//                xxx - sign us that the key should come first - to store the val for every matching 3grams
                Text key = new Text(line_as_string[0] + " " + line_as_string[1] + " " +line_as_string[2]);
                Text val = new Text(line_as_string[3] + " " +line_as_string[4]);
                context.write(key,val);
            }
        }

    }

    public static class ReducerClass extends Reducer<Text,Text,Text,Text> {


        String on_going_three_gram = null;
        long nr = 0;
        long tr = 0;
        private long N;



        @Override
        protected void setup(Reducer<Text,Text,Text,Text>.Context context) throws IOException, InterruptedException {
            N = context.getConfiguration().getLong("counter", -1);
            System.out.println("==========================================================================="+N);
        }


        @Override
        protected void reduce(Text key, Iterable<Text> values, Reducer<Text,Text,Text,Text>.Context context) throws IOException, InterruptedException {

//        @Override
//        protected void cleanup(Reducer<Text,ArrayWritable,Text,ArrayWritable>.Context context) throws IOException, InterruptedException {
//        }

            String key_string = key.toString();
            for (Text value : values) {
                String []value_to_string = value.toString().split("\\s+");
                long to_add_nr = Long.parseLong(value_to_string[0]);
                long to_add_tr = Long.parseLong(value_to_string[1]);
                if(key_string.equals(on_going_three_gram)){
                    nr += to_add_nr;
                    tr += to_add_tr;
                    System.out.println(on_going_three_gram +" key didn't change: "+ nr + " /" + tr);
                }
                else if(on_going_three_gram != null){
//                    TODO find out how to set and use N
                    double den = (double) (N) * (double) (tr);

                    double probability = (double)(nr) / den;
                    Text probability_text = new Text(String.valueOf(probability));
                    System.out.println(on_going_three_gram +" probability: "+ nr + " /" + tr + "=" + probability);

                    context.write(new Text(on_going_three_gram), probability_text);
                    on_going_three_gram = key_string;
                    nr = to_add_nr;
                    tr = to_add_tr;
                }
//                first run ==> on_going_three_gram == null
                else{
                    on_going_three_gram = key_string;
                    nr = to_add_nr;
                    tr = to_add_tr;
                }

            }

        }

    }

//
//    //    Partitioner
//    public static class PartitionerClass extends Partitioner<Text, IntWritable> {
//        @Override
//        public int getPartition(Text key, IntWritable value, int numPartitions) {
//            String part = key.toString().split("\\s+")[0];
//            return part.hashCode() % numPartitions;
//        }
//    }


//
//    public static void main(String[] args) throws Exception {
//        Configuration conf = new Configuration();
//        Job job = Job.getInstance(conf, "calculate_probability");
//        job.setJarByClass(step3_calculate_probability.class);
//        job.setMapperClass(Mapper_pass_to_reducer.class);
////        job.setPartitionerClass(PartitionerClass.class);
////        job.setCombinerClass(ReducerClass.class);
//        job.setReducerClass(ReducerClass.class);
//        job.setMapOutputKeyClass(Text.class);
//        job.setMapOutputValueClass(Text.class);
//        job.setOutputKeyClass(Text.class);
//        System.out.println("========================================");
//
//        job.setOutputValueClass(Text.class);
////        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class,Mapper_pass_to_reducer.class);
////        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class,Mapper_pass_to_reducer.class);
//        FileInputFormat.addInputPath(job, new Path(args[0]));
//        FileOutputFormat.setOutputPath(job, new Path(args[1]));
////        job.setInputFormatClass(SequenceFileAsTextInputFormat.class);
//        job.setInputFormatClass(TextInputFormat.class);
//        job.setOutputFormatClass(TextOutputFormat.class);
//        System.exit(job.waitForCompletion(true) ? 0 : 1);
//    }

}