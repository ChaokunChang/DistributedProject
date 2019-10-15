public class DistComputing {
    public static double threshold = 500;
    public static double dist(String a,String b){
        String[] x_vec = a.split(",");
        String[] y_vec = b.split(",");
        int len = x_vec.length;
        double sum = 0;
        double x_a = 0;
        double y_a = 0;
        for(int i = 0; i < len; i++){
            x_a = Double.parseDouble(x_vec[i]);
            y_a = Double.parseDouble(y_vec[i]);
            sum += (x_a-y_a)*(x_a-y_a);
        }
//        double x_a = Double.parseDouble((a.split(","))[0]);
//        double y_a = Double.parseDouble((a.split(","))[1]);
//        double x_b = Double.parseDouble((b.split(","))[0]);
//        double y_b = Double.parseDouble((b.split(","))[1]);
//        double dis = Math.sqrt((x_a-x_b)*(x_a-x_b)+(y_a-y_b)*(y_a-y_b));
        return Math.sqrt(sum);
    }
}
