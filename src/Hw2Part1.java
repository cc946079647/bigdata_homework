import java.io.IOException;
import java.text.DecimalFormat;
import java.util.StringTokenizer;
import java.nio.charset.CharacterCodingException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class Hw2Part1{
    public static class TokenizerMapper
            extends Mapper<Object,Text,Text,DoubleWritable>{
        private Text pairs = new Text();
        //split sring
        private String splitStr = " ";
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer tkn = new StringTokenizer(value.toString());
            //handle input only when it's valid
            if(tkn.countTokens() == 3){
                String first = tkn.nextToken();
                String second = tkn.nextToken();
                String metric = tkn.nextToken();
                //System.out.println(first+" "+second+" "+metric);
                pairs.set(first+splitStr+second);

                context.write(pairs,new DoubleWritable(Double.valueOf(metric)));
            }
        }
    }

    public static class DoubleSumReducer
            extends Reducer<Text,DoubleWritable,Text,Text>{
        //control average format
        private DecimalFormat avgFormat = new DecimalFormat("#.000");
        private Text resultKey = new Text();
        private Text resultValue = new Text();
        byte[] split;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            try{
                split = Text.encode(" ").array();
            }catch (Exception ex){
                ex.printStackTrace();
            }
        }

        @Override
        protected void reduce(Text key, Iterable<DoubleWritable> values, Context context)
                throws IOException, InterruptedException {
            double sum = 0.0;
            int count = 0;
            for(DoubleWritable dbw : values){
                sum += dbw.get();
                count++;
            }
            //calculate average
            double avg = sum / count;

            String con_pair = key.toString();
            String[] pairs = con_pair.split(" ");
            String first = pairs[0];
            String second = pairs[1];
            String avgStr = avgFormat.format(avg);
            System.out.println(first+" "+second);
            System.out.println(count+" "+avg);

            resultKey.set(first);
            resultKey.append(split,0,split.length);
            resultKey.append(second.getBytes(),0,second.getBytes().length);

            resultValue.set(String.valueOf(count));
            resultValue.append(split,0,split.length);
            resultValue.append(avgStr.getBytes(),0,avgStr.getBytes().length);

            context.write(resultKey,resultValue);

        }
    }

    static String inputFilePath = null;
    static String outFilePath = null;

    public static void main(String[]args)throws IllegalArgumentException,IOException,InterruptedException,ClassNotFoundException{
        //check input number
        if(args.length != 2){
            System.err.println("Usage:Hw2Part1 <input> <output>");
            throw new IllegalArgumentException("there should be 2 parameters!");
        }
        inputFilePath = args[0];
        outFilePath = args[1];

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Hw2Part1");

        job.setJarByClass(Hw2Part1.class);

        job.setMapperClass(TokenizerMapper.class);
        job.setReducerClass(DoubleSumReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);


        FileInputFormat.addInputPath(job, new Path(inputFilePath));
        FileOutputFormat.setOutputPath(job, new Path(outFilePath));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }


}