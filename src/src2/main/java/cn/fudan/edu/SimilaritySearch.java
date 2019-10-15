import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class SimilaritySearch {
    public static void main(String[] args) throws Exception{
        CentroidSampling cs = new CentroidSampling();
        String centroids = cs.sampling();
        Configuration conf = new Configuration();
        conf.set(CentroidSampling.FLAG_CENTROID,centroids);

        //job1
        Job job1 = new Job(conf,"CentroidStats");
        job1.setJarByClass(SimilaritySearch.class);
        job1.setMapperClass(CentroidStatsMapper.class);
        job1.setReducerClass(CentroidStatsReducer.class);
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(DoubleWritable.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);

//        ControlledJob ctrl_job1 = new ControlledJob(conf);
//        ctrl_job1.setJob(job1);
        FileInputFormat.addInputPath(job1,new Path(args[0]));
        FileOutputFormat.setOutputPath(job1,new Path(args[1]));

        //Job2


        if(job1.waitForCompletion(true)){
            Center center = new Center();
            String centers = center.loadCenter(new Path(args[1]));
//            System.out.println("44444");
//            System.out.println(centers);
            conf.set(Center.CENTERFLAG,centers);
            Job job2 = new Job(conf,"Similarity");
            job2.setJarByClass(SimilaritySearch.class);
            job2.setMapperClass(SimilarityMapper.class);
            job2.setReducerClass(SimilarityReducer.class);
            job2.setMapOutputKeyClass(Text.class);
            job2.setMapOutputValueClass(Text.class);
            job2.setOutputKeyClass(Text.class);
            job2.setOutputValueClass(Text.class);

//        ControlledJob ctrl_job2 = new ControlledJob(conf);
//        ctrl_job2.setJob(job2);
//        ctrl_job2.addDependingJob(ctrl_job1);
            FileInputFormat.addInputPath(job2,new Path(args[0]));
            FileOutputFormat.setOutputPath(job2,new Path(args[2]));
            System.exit(job2.waitForCompletion(true)?0:1);
        }
//        JobControl job_ctrl = new JobControl("SimilaritySearch");
//        job_ctrl.addJob(ctrl_job1);
//        job_ctrl.addJob(ctrl_job2);
//
//        Thread t = new Thread(job_ctrl);
//        t.start();
//
//        while (true){
//            if(job_ctrl.allFinished()){
//                System.out.println(job_ctrl.getSuccessfulJobList());
//                job_ctrl.stop();
//                break;
//            }
//        }
    }
}
