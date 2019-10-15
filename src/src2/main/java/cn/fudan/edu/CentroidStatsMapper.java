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

public class CentroidStatsMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {

    private String[] centroids;
    private int centroids_size = 0;
    @Override
    public void setup(Context context){
        String centroid_string = context.getConfiguration().get(CentroidSampling.FLAG_CENTROID);
        centroids = centroid_string.split("\t");
        centroids_size = centroids.length;
    }

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException,InterruptedException{
        String val = value.toString();
        double min_distance = Double.MAX_VALUE; //set as max DOUBLE
        double tem_distance = 0;
        int min_id = 0;
        for(int i = 0; i < centroids_size; i++){
            tem_distance = DistComputing.dist(centroids[i],val);
            if(tem_distance < min_distance){
                min_id = i;
                min_distance = tem_distance;
            }
        }
        context.write(new Text(min_id+"#"+centroids[min_id]),new DoubleWritable(min_distance));
    }
}