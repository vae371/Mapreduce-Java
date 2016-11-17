package complete;

import java.io.BufferedWriter;

import java.io.File;
import java.io.FileWriter;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class IterReduce extends Reducer<Text, Text, Text, Text> {

	/**
	 * Reducer do the actual matrix multiplication.
	 *
	 * @param key
	 *            is the cell unique cell dimension (00) represents cell 0,0
	 * @value values required to calculate matrix multiplication result of that
	 *        cell.
	 */

	static double crossRate;
	static double mutRate;
	static Context context;

	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

		this.context = context;

		crossRate = Double.parseDouble(context.getConfiguration().get("crossRate"));
		mutRate = Double.parseDouble(context.getConfiguration().get("mutRate"));

		ArrayList<StringBuilder> pool = new ArrayList(2);

		for (Text val : values) {
			StringBuilder sb = new StringBuilder();
			sb.append(val.toString());
			pool.add(sb);
		}

		StringBuilder sb1 = pool.get(0);
		StringBuilder sb2 = pool.get(1);
		String decodedString1 = decodeChromo(sb1.toString());
		String decodedString2 = decodeChromo(sb2.toString());
		write(decodedString1,sb1.toString());
		write(decodedString2,sb2.toString());

		crossOver(sb1, sb2);
		mutate(sb1);
		mutate(sb2);
		decodedString1 = decodeChromo(sb1.toString());
		decodedString2 = decodeChromo(sb2.toString());
		write(decodedString1,sb1.toString());
		write(decodedString2,sb2.toString());

	

		context.write(new Text(sb1.toString()), new Text(""));
		context.write(new Text(sb2.toString()), new Text(""));

	}

	public void write(String decodedString,String sb) throws IOException {
		if (compute(decodedString) == Integer.parseInt(this.context.getConfiguration().get("target"))
				&& isValid(decodedString)) {
			
			String fileName = context.getConfiguration().get("result_path");
			
			File f=new File(fileName);
			// Assume default encoding.
			FileWriter fileWriter = new FileWriter(fileName);
			// Always wrap FileWriter in BufferedWriter.
			BufferedWriter bufferedWriter = new BufferedWriter(fileWriter);
			// PrintWriter out = new PrintWriter(bufferedWriter);
			bufferedWriter.write(sb+","+decodedString+","+this.context.getConfiguration().get("target"));
		
			bufferedWriter.write("");
			bufferedWriter.close();
			fileWriter.close();
		}
//		else{
////			String fileName = "try.txt";
////			// Assume default encoding.
////			FileWriter fileWriter = new FileWriter(fileName);
////			// Always wrap FileWriter in BufferedWriter.
////			BufferedWriter bufferedWriter = new BufferedWriter(fileWriter);
////			PrintWriter out = new PrintWriter(bufferedWriter);
////			 out.println(compute(decodedString)+","+this.context.getConfiguration().get("target"));
//			System.out.println(compute(decodedString)+","+this.context.getConfiguration().get("target")+","+decodedString);
//			
//			
//		}
	}

	public final void crossOver(StringBuilder sb1, StringBuilder sb2) {
		Random rand = new Random();
		// Should we cross over?
		if (rand.nextDouble() > crossRate)
			return;

		// Generate a random position
		int pos = rand.nextInt(sb1.length());

		// Swap all chars after that position
		for (int x = pos; x < sb1.length(); x++) {
			// Get our character
			char tmp = sb1.charAt(x);

			// Swap the chars
			sb1.setCharAt(x, sb2.charAt(x));
			sb1.setCharAt(x, tmp);
		}
	}

	public final void mutate(StringBuilder sb1) {
		Random rand = new Random();
		for (int x = 0; x < sb1.length(); x++) {
			if (rand.nextDouble() <= mutRate)
				sb1.setCharAt(x, (sb1.charAt(x) == '0' ? '1' : '0'));
		}
	}

	public final String decodeChromo(String chromo) {

		// Create a buffer
		StringBuffer decodeChromo = new StringBuffer();
		decodeChromo.setLength(0);
		char[] ltable = { '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', '+', '-', '*', '/' };
		// Loop throught the chromo
		for (int x = 0; x < chromo.length(); x += 4) {
			// Get the
			int idx = Integer.parseInt(chromo.substring(x, x + 4), 2);
			if (idx < ltable.length)
				decodeChromo.append(ltable[idx]);
		}

		// Return the string
		return decodeChromo.toString();
	}

	public final boolean isValid(String decodedString) {

		boolean num = true;
		for (int x = 0; x < decodedString.length(); x++) {
			char ch = decodedString.charAt(x);

			// Did we follow the num-oper-num-oper-num patter
			if (num == !Character.isDigit(ch))
				return false;

			// Don't allow divide by zero
			if (x > 0 && ch == '0' && decodedString.charAt(x - 1) == '/')
				return false;

			num = !num;
		}

		// Can't end in an operator
		if (!Character.isDigit(decodedString.charAt(decodedString.length() - 1)))
			return false;

		return true;
	}

	public int compute(String decodedString) {
		// Total
		int tot = 0;

		// Find the first number
		int ptr = 0;
		while (ptr < decodedString.length()) {
			char ch = decodedString.charAt(ptr);
			if (Character.isDigit(ch)) {
				tot = ch - '0';
				ptr++;
				break;
			} else {
				ptr++;
			}
		}

		// If no numbers found, return
		if (ptr == decodedString.length()) {

			return 0;
		}

		// Loop processing the rest
		boolean num = false;
		char oper = ' ';
		while (ptr < decodedString.length()) {
			// Get the character
			char ch = decodedString.charAt(ptr);

			// Is it what we expect, if not - skip
			if (num && !Character.isDigit(ch)) {
				ptr++;
				continue;
			}
			if (!num && Character.isDigit(ch)) {
				ptr++;
				continue;
			}

			// Is it a number
			if (num) {
				switch (oper) {
				case '+': {
					tot += (ch - '0');
					break;
				}
				case '-': {
					tot -= (ch - '0');
					break;
				}
				case '*': {
					tot *= (ch - '0');
					break;
				}
				case '/': {
					if (ch != '0')
						tot /= (ch - '0');
					break;
				}
				}
			} else {
				oper = ch;
			}

			// Go to next character
			ptr++;
			num = !num;
		}
		return tot;
	}

}
