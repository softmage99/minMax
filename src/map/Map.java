package map;

import java.io.IOException;

import main.vo;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class Map extends Mapper<LongWritable, Text, Text, vo>{

	
	private Text outUserId = new Text();
	private vo vo = new vo();
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		System.out.println("map1 start");
		if(value!=null){
			String line = value.toString();
			String data[] = line.split(",");
			if(data[0]!=null && data.length==9){
				outUserId.set(data[0]);
				if(data[6]!=null && data[7]!=null && !data[6].equals("MIN Price")){
					Double val = Double.parseDouble(data[7])-Double.parseDouble(data[6]);
					vo.setMin(val);
					vo.setMax(val);
					vo.setCtr(1);
					context.write(outUserId, vo);
				}
			}
			
		}
		
		System.out.println("map1 end");
		
	}

}
