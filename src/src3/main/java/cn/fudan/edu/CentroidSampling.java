import java.util.Random;

public class CentroidSampling {
    public static String FLAG_CENTROID = "FLAG_CENTROID";
    protected static int num_samples = 50;
    protected Double[] uphold = new Double[20];
    protected Double[] downhold = new Double[20];

    protected double min1 = 0;
    protected double max1 = 500;
    protected double min2 = 0;
    protected double max2 = 500;

    public String make_random(Double[] min, Double[] max){
        String randstr = new String();
        for(int i=0;i<20;i++){
            randstr = randstr+(min[i] + (max[i]-min[i])*new Random().nextDouble());
            if(i<19) randstr = randstr + ",";
        }
        return randstr;
    }
    public String sampling(){
        for(int i=0; i<20; i++){
            uphold[i]=500.0;
            downhold[i]=0.0;
        }
        StringBuffer sb = new StringBuffer();
        String[] centroids_string = new String[num_samples];
        for(int i = 0; i < num_samples;i++){
            int j = 0;
            centroids_string[i] = make_random(downhold, uphold);
            while (j < i){
                if(DistComputing.dist(centroids_string[i],centroids_string[j]) <= DistComputing.threshold){
                    j = 0;
                    centroids_string[i] = make_random(downhold, uphold);
                }
                else j++;
            }
        }

        for(int i = 0; i< num_samples; i++){
            sb.append(centroids_string[i]);
            sb.append("\t");
        }
        return sb.toString().trim();
    }
}
