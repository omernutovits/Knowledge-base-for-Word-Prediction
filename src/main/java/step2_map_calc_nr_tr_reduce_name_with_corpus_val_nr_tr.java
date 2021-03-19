import java.io.IOException;

//s3://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/3gram/data

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;


public class step2_map_calc_nr_tr_reduce_name_with_corpus_val_nr_tr {
    public static Counter counter;


    public static class Mapper_Nr_Tr extends Mapper<LongWritable, Text, Text, Text> {

//        LongWritable one;


        @Override
        protected void setup(Mapper<LongWritable, Text, Text, Text>.Context context) {

//            one = new LongWritable(1);
        }

        @Override
        public void map(LongWritable lineId, Text line, Context context) throws IOException,  InterruptedException {
            String[] line_as_string = line.toString().split("\\s+");
            Text key_one ;
            Text key_two ;

            System.out.println("Map::::" + lineId.toString() + ". " + line.toString() + "");
            if (line_as_string.length > 3) {

//                LongWritable occurrences_first_corpus = new LongWritable(Long.parseLong(line_as_string[3]));
//                Text occurrences_second_corpus = new Text("01 " + line_as_string[4]);
                String occurrences_one = line_as_string[3];
                key_one = new Text( occurrences_one + "_1" + " xxx" );
                context.write(key_one,new Text("0 "+ line_as_string[4]));
//                context.write(key_one,occurrences_second_corpus);

//                Text occurrences_first_corpus = new Text("10 " + line_as_string[3]);
                String occurrences_two = line_as_string[4];
                key_two = new Text( occurrences_two + "_2"+ " xxx" );
                context.write(key_two,new Text("1 " + line_as_string[3]));
//                context.write(key_two,occurrences_first_corpus);

                Text key_corpus_one = new Text(line_as_string[3] + "_1 zzz");
                Text key_corpus_two = new Text(line_as_string[4] + "_2 zzz");
                Text val = new Text(line_as_string[0] + " " +line_as_string[1] + " " +line_as_string[2]);
                context.write(key_corpus_one,val);
                context.write(key_corpus_two,val);



            }
        }

    }


    public static class ReducerClass extends Reducer<Text,Text,Text,Text> {

        Text value_to_write = null;
//        Text last_key_of_three_gram = null;



//        @Override
//        protected void setup(Reducer<Text,ArrayWritable,Text,ArrayWritable>.Context context) throws IOException, InterruptedException {
//        }

        @Override
        protected void reduce(Text key, Iterable<Text> values, Reducer<Text,Text,Text,Text>.Context context) throws IOException, InterruptedException {
            counter = context.getCounter(all_steps.NCounter.N_COUNTER);
            long nr = 0;
            long tr = 0;
            long fails = 0;

            if (key.toString().contains("zzz")) {
                for (Text value : values) {
                    context.write(value, value_to_write);
                }
            } else {
                for (Text value : values) {
                    String val_string = value.toString();
                    String type_of_value = val_string.substring(0, val_string.indexOf(" "));
                    long actual_value = Long.parseLong(val_string.substring(val_string.indexOf(" ") + 1));
//                    boolean is_nr = type_of_value.equals("0") || type_of_value.equals("1");
//                    boolean is_tr = type_of_value.equals("01") || type_of_value.equals("10");
//                    if (is_nr) {
                        nr += 1;
//                    } else if (is_tr) {
                        tr += actual_value;
//                    } else {
//                        fails += 1;
//                    }

                }
//                Text text_value = new Text(nr + " " + tr + " " + fails);
//                context.write(key, text_value);
                value_to_write = new Text(nr + " " + tr + " " + fails);
            }
        }

    }

    //    Partitioner
    public static class PartitionerClass extends Partitioner<Text, Text> {
        @Override
        public int getPartition(Text key, Text value, int numPartitions) {
            String part = key.toString().split("\\s+")[0];
            Text newKey = new Text(part);
            return Math.abs(newKey.hashCode()) % numPartitions;
        }
    }


//
//    public static void main(String[] args) throws Exception {
//        Configuration conf = new Configuration();
//        Job job = Job.getInstance(conf, "step2_map_calc_nr_tr_with_3grams");
//        job.setJarByClass(step2_map_calc_nr_tr_reduce_name_with_corpus_val_nr_tr.class);
//        job.setMapperClass(Mapper_Nr_Tr.class);
//        job.setPartitionerClass(PartitionerClass.class);
////        job.setCombinerClass(ReducerClass.class);
//        job.setReducerClass(ReducerClass.class);
//        job.setMapOutputKeyClass(Text.class);
//        job.setMapOutputValueClass(Text.class);
//        job.setOutputKeyClass(Text.class);
//        System.out.println("========================================");
//
//        job.setOutputValueClass(Text.class);
////        MultipleInputs.addInputPath(job, new Path(args[0]),
////                TextInputFormat.class, Mapper_Nr_Tr.class);
////        MultipleInputs.addInputPath(job, new Path(args[1]),
////                TextInputFormat.class, Mapper_exchange_three_gram_key_with_val_per_corpus.class);
//        FileInputFormat.addInputPath(job, new Path(args[0]));
//        FileOutputFormat.setOutputPath(job, new Path(args[1]));
////        job.setInputFormatClass(SequenceFileAsTextInputFormat.class);
////        job.setInputFormatClass(TextInputFormat.class);
//        job.setOutputFormatClass(TextOutputFormat.class);
//        System.exit(job.waitForCompletion(true) ? 0 : 1);
//    }

}