import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.LineReader;

public class Center {

	public String printCenter(Path path) throws IOException{
		StringBuffer sBuffer = new StringBuffer();
		
		Configuration configuration = new Configuration();
		FileSystem hdfs = FileSystem.get(configuration);
		FSDataInputStream dis = hdfs.open(path);
		LineReader in = new LineReader(dis, configuration);
		Text line = new Text();
		System.out.println("preCenter:" + line);
		while(in.readLine(line) > 0){
			sBuffer.append(line.toString().trim());
			sBuffer.append("\n");
		}
		return sBuffer.toString().trim();
	}
	

	public String printNewCenter(Path path) throws IOException{
		StringBuffer sBuffer = new StringBuffer();
		
		Configuration configuration = new Configuration();
		FileSystem hdfs = FileSystem.get(configuration);
		FileStatus[] files = hdfs.listStatus(path);
		
		for (int i = 0; i < files.length; i++){
			Path filePath = files[i].getPath();
			if(!filePath.getName().contains("part")) continue;
			FSDataInputStream dis = hdfs.open(filePath);
			LineReader in = new LineReader(dis,configuration);
			Text line = new Text();
			while(in.readLine(line) > 0){
				sBuffer.append(line.toString().trim());
				sBuffer.append("\n");
			}
		}
		return sBuffer.toString().trim();
	}
}