import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.Arrays;
import java.util.Scanner;


public class IMDBMAPR {

    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        @Override
        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {

                Scanner sc = new Scanner(value.toString());
                sc.useDelimiter(";");

                //get movieId
                String movieTitleId = sc.next();
                //get movieType
                String type = sc.next();
                String movieTitle = sc.next();
                String movieYr = sc.next();

                String genre_ip = sc.next();
                String[] genre_arr = genre_ip.split(",");

                Text movieText = new Text();
                movieText.set(genre_ip);

                String[] original1 = {"Comedy","Romance"};
                String[] original2 = {"Action", "Thriller"};
                String[] original3 = {"Adventure", "Sci-Fi"};


                if(!movieTitle.contentEquals(";") && type.contentEquals("movie") && movieYr.matches("[0-9]+")) {

                    int movieYear = 0;


                        movieYear = Integer.parseInt(movieYr);
                        String genre = "";
                        String yearRange = "";

                        if(Arrays.asList(genre_arr).containsAll(Arrays.asList(original1))){
                            genre = "Comedy;Romance";
                        }
                        else if(Arrays.asList(genre_arr).containsAll(Arrays.asList(original2))) {
                            genre = "Action;Thriller";
                        }
                        else if(Arrays.asList(genre_arr).containsAll(Arrays.asList(original3))){
                            genre = "Adventure;Sci-Fi";
                        }


                        if(genre!="") {

                            if(movieYear>=2001 && movieYear<=2005)
                            {
                                yearRange ="[2001-2005]";
                            }
                            else if(movieYear >= 2006 && movieYear<=2010)
                            {
                                yearRange ="[2006-2010]";
                            }
                            else if(movieYear >=2011 && movieYear<=2015)
                            {
                                yearRange ="[2011-2015]";
                            }


                            Text finalText = new Text();
                            String resultVal = yearRange + " " + genre;

                            Text resultKey = new Text();
                            resultKey.set(resultVal);

                            if(yearRange!= "") {
                                context.write(resultKey, new IntWritable(1));
                            }
                    }


                }
        }
    }


    public static class IntSumReducer
            extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            int count = 0;
            for(IntWritable val:values)
            {
                count = count + val.get();
            }
            context.write(key, new IntWritable(count));
        }
    }


        public static void main(String[] args) throws Exception {


            Configuration conf = new Configuration();


            Job job = Job.getInstance(conf, "word count");

            job.setJarByClass(IMDBMAPR.class);

            job.setMapperClass(TokenizerMapper.class);

            job.setCombinerClass(IntSumReducer.class);

            job.setReducerClass(IntSumReducer.class);

            job.setMapOutputKeyClass(Text.class);

            job.setMapOutputValueClass(IntWritable.class);

            job.setOutputKeyClass(Text.class);

            job.setOutputValueClass(IntWritable.class);

            job.setInputFormatClass(TextInputFormat.class);

            job.setOutputFormatClass(TextOutputFormat.class);

            FileInputFormat.addInputPath(job, new Path(args[0]));

            FileOutputFormat.setOutputPath(job, new Path(args[1]));

            System.exit(job.waitForCompletion(true) ? 0 : 1);
        }
    }
