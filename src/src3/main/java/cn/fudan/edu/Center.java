import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.LineReader;

import java.io.IOException;

public class Center {
    public static String CENTERFLAG = "CENTERFLAG";
    public String loadCenter(Path path) throws IOException{
        StringBuffer sb = new StringBuffer();
        Configuration conf = new Configuration();
        FileSystem hdfs = FileSystem.get(conf);
        FileStatus[] files = hdfs.listStatus(path);

        for(int i = 0; i < files.length; i++){
            Path filePath = files[i].getPath();
            System.out.println("000000000000000000000000000000000\n");
            System.out.println(filePath);
            if(!filePath.getName().contains("part")) continue;
            FSDataInputStream dis = hdfs.open(filePath);
            LineReader in = new LineReader(dis,conf);
            Text line = new Text();
            while(in.readLine(line) > 0){
                sb.append(line.toString().trim());
                sb.append("\t");
            }
        }
        //System.out.println("333333333333333333333333333333333\n");
        //System.out.println(sb.toString().trim());
        return sb.toString().trim();
    }
}
