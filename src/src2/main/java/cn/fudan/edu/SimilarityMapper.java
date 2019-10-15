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

public class SimilarityMapper extends Mapper<LongWritable, Text, Text, Text>{
    protected String[] centroids = new String[CentroidSampling.num_samples];
    protected Double[] radii = new Double[CentroidSampling.num_samples];
    protected int centroids_size;

    @Override
    protected void setup(Context context){
        String centroid_string = context.getConfiguration().get(Center.CENTERFLAG);
        System.out.println("11111111111\n");
        System.out.println(centroid_string);
        String[] centroids_2 = centroid_string.split("\t");
        centroids_size = centroids_2.length;
        for(int i = 0; i < centroids_size; i++){
            //System.out.println(centroids_2[i]);
            //System.out.println((centroids_2[i].split("#"))[0]);
            //System.out.println((centroids_2[i].split("#"))[1]);
            //System.out.println((centroids_2[i].split("#"))[2]);
            centroids[i] = (centroids_2[i].split("#"))[1];

            radii[i] = Double.parseDouble((centroids_2[i].split("#"))[2]);
        }
    }
    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException,InterruptedException{
        String val = value.toString();
        //get the partition of the point.
        int Par = 0;
        double tem_dist=0;
        double par_dist = Double.MAX_VALUE;
        for(int i=0; i<centroids_size; i++){
            tem_dist = DistComputing.dist(val,centroids[i]);
            if(tem_dist<par_dist){
                Par = i;
                par_dist = tem_dist;
            }
        }

        //Route the point to single partition
        for(int i = 0; i < centroids_size; i++){
            if(i==Par){
                context.write(new Text(centroids[i]),new Text(val+"#inner#"+tem_dist));
                //context.write(new Text(centroids[i]),new Text("inner"),new Text(val+"#inner#"));
            }
            else{
                Boolean criterion = ((i+Par)%2 == 1) ^ (Par<i);
                tem_dist = DistComputing.dist(val,centroids[i]);
                if(criterion && (tem_dist < radii[i] + DistComputing.threshold)){
                    context.write(new Text(centroids[i]),new Text(val+"#outer#"+tem_dist));
                    //context.write(new Text(centroids[i]),new Text("outer"),new Text(val+"#outer#"));
                }

            }
        }
    }
}
