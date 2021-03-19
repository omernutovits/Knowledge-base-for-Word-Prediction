import java.io.IOException;

//s3://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/3gram/data

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;


public class step4_sort {



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
                double prob_ascending = 1.0 - Double.parseDouble(line_as_string[3]);
//                xxx - sign us that the key should come first - to store the val for every matching 3grams
                Text key = new Text(line_as_string[0] + " " + line_as_string[1] + " " + prob_ascending + " " + line_as_string[2]);
                Text val = new Text();
                context.write(key,val);
            }
        }

    }

    public static class ReducerClass extends Reducer<Text,Text,Text,Text> {



        @Override
        protected void reduce(Text key, Iterable<Text> values, Reducer<Text,Text,Text,Text>.Context context) throws IOException, InterruptedException {


            String [] key_string = key.toString().split("\\s+");
            String three_gram = key_string[0] + " " + key_string[1] + " " + key_string[3];
            double prob = 1.0 - Double.parseDouble(key_string[2]);
            for (Text value : values) {
                context.write(new Text(three_gram), new Text(String.valueOf(prob)));
//                first run ==> on_going_three_gram == null

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
//        Job job = Job.getInstance(conf, "step4_sort");
//        job.setJarByClass(step4_sort.class);
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