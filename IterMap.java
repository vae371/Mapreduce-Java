package complete;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class IterMap  extends Mapper  <LongWritable, Text, Text, Text> {
	
	
	
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {		
		
		Text Key=new Text(key.get()/60+"");
		String chomo=value.toString().trim();		
		
		context.write(Key, new Text(chomo));
	}
	
	public final String decodeChromo(String chromo) {	

		// Create a buffer
		StringBuffer decodeChromo=new StringBuffer();
		decodeChromo.setLength(0);
		char[] ltable = { '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', '+', '-', '*', '/' };
		// Loop throught the chromo
		for (int x=0;x<chromo.length();x+=4) {
			// Get the
			int idx = Integer.parseInt(chromo.substring(x,x+4), 2);
			if (idx<ltable.length) decodeChromo.append(ltable[idx]);
		}
		
		// Return the string
		return decodeChromo.toString();
	}
	
	public final boolean isValid(String decodedString) { 
		
		
		
		boolean num = true;
		for (int x=0;x<decodedString.length();x++) {
			char ch = decodedString.charAt(x);

			// Did we follow the num-oper-num-oper-num patter
			if (num == !Character.isDigit(ch)) return false;
			
			// Don't allow divide by zero
			if (x>0 && ch=='0' && decodedString.charAt(x-1)=='/') return false;
			
			num = !num;
		}
		
		// Can't end in an operator
		if (!Character.isDigit(decodedString.charAt(decodedString.length()-1))) return false;
		
		return true;
	}
	
	public int compute(String decodedString){
		// Total
				int tot = 0;
				
				// Find the first number
				int ptr = 0;
				while (ptr<decodedString.length()) { 
					char ch = decodedString.charAt(ptr);
					if (Character.isDigit(ch)) {
						tot=ch-'0';
						ptr++;
						break;
					} else {
						ptr++;
					}
				}
				
				// If no numbers found, return
				if (ptr==decodedString.length()){
					
					return 0;
				}
				
				// Loop processing the rest
				boolean num = false;
				char oper=' ';
				while (ptr<decodedString.length()) {
					// Get the character
					char ch = decodedString.charAt(ptr);
					
					// Is it what we expect, if not - skip
					if (num && !Character.isDigit(ch)) {ptr++;continue;}
					if (!num && Character.isDigit(ch)) {ptr++;continue;}
				
					// Is it a number
					if (num) { 
						switch (oper) {
							case '+' : { tot+=(ch-'0'); break; }
							case '-' : { tot-=(ch-'0'); break; }
							case '*' : { tot*=(ch-'0'); break; }
							case '/' : { if (ch!='0') tot/=(ch-'0'); break; }
						}
					} else {
						oper = ch;
					}			
					
					// Go to next character
					ptr++;
					num=!num;
				}
				return tot;
	}
	
	

		

	
}
