import java.io.IOException;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class SimilarityReducer extends Reducer<Text,Text,Text,Text>{
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
        throws IOException,InterruptedException{
        String[] value_string = new String[100000];
        int i = 0;
        System.out.println("now we are in redeuce.");
        for(Text value: values){
            value_string[i] = value.toString();
            System.out.println(i);
            i++;
        }
        int size = i;
        //System.out.println("ssssssssssiiiiiiiiiiiiiizzzzzzzzzzzzzzeeeeeeeeeeeeee");
        //System.out.println(size);
        for(int j = 0; j < size; j++){
            String[] tem = value_string[j].split("#");
//            if(tem[1].equals("outer")){
//                System.out.println(tem[0]);
//                System.out.println(tem[1]);
//            }

            if(tem[1].equals("inner")){
                for(int k = j+1; k < size; k++){
                    String tem2 = value_string[k].split("#")[0];
//                    System.out.println(DistComputing.dist(tem[0],tem2));
                    if(DistComputing.dist(tem[0],tem2) < DistComputing.threshold){
                        context.write(new Text(tem[0]+"#"+tem2),new Text(""));
                    }
                }
            }
        }
    }

}
