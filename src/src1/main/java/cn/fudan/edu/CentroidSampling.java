import java.util.Random;

public class CentroidSampling {
    public static String FLAG_CENTROID = "FLAG_CENTROID";
    protected static int num_samples = 350;
    protected Double[] uphold = new Double[91];
    protected Double[] downhold = new Double[91];
    
    protected double min1 = 0;
    protected double max1 = 300;
    protected double min2 = 0;
    protected double max2 = 300;

    public String make_random(double min1, double max1, double min2, double max2){
        return ((min1 + (max1-min1)*new Random().nextDouble())+","+(min2 + (max2-min2)*new Random().nextDouble()));
    }
    public String sampling(){
        StringBuffer sb = new StringBuffer();
        String[] centroids_string = new String[num_samples];
        for(int i = 0; i < num_samples;i++){
            int j = 0;
            centroids_string[i] = make_random(min1,max1,min2,max2);
            while (j < i){
                if(DistComputing.dist(centroids_string[i],centroids_string[j]) <= DistComputing.threshold){
                    j = 0;
                    centroids_string[i] = make_random(min1,max1,min2,max2);
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
