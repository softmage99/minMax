package reduce;


import java.io.IOException;

import main.vo;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Reduce extends Reducer<Text, vo, Text, vo>{

	private vo vo = new vo();
	
	public void reduce(Text key, Iterable<vo> values,
			Context context) throws IOException, InterruptedException {
		
		System.out.println("reduce start");
		
		int sum = 0;
		int i=0;
		
		for(vo vo_val : values){
			
			if(i==0){
				vo.setMax(vo_val.getMin());
				vo.setMin(vo_val.getMax());
				vo.setCtr(vo_val.getCtr());
				i+=1;
			}else {
				if(vo.getMin()==0 || 
						vo_val.getMin()<vo.getMin()){
					
					vo.setMin(vo_val.getMin());
					
				}
				
				if(vo.getMax()==0 || 
						vo_val.getMax()>vo.getMax()){
					
					vo.setMax(vo_val.getMax());
					
				}
				
			}
			sum += vo_val.getCtr();
			
			
		}
		System.out.println(key+"="+sum);
		vo.setCtr(sum);
		
		context.write(key, vo);
		System.out.println("end reduce");
	}
}
