package parser;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

public class MatrixParser {

	public void saveOutput() throws IOException
	{
		BufferedWriter writer = 
				new BufferedWriter(new FileWriter("./data/gre_343.txt"));

		BufferedReader sc =
				new BufferedReader(new FileReader("./data/gre_343.mtx"));
		
		StringBuffer strBuffer = new StringBuffer();

		String line = sc.readLine();
		
		boolean condition = true;

		while(condition) {

			if(line.contains("%")) {
				line = sc.readLine();
				continue;
			}

			String[] seqsplit = line.split(" ");
			strBuffer.append(seqsplit[0] + " ");
			strBuffer.append(seqsplit[1] + "\n");
			System.out.println(seqsplit[0] + " " + seqsplit[1]);

			String currentLine = sc.readLine();

			if(currentLine == null) {
				condition = false;
			}else {
				line = currentLine;
			}
		}
		writer.write(strBuffer.toString());
		writer.flush();		
		writer.close();
		sc.close();
	}


	public static void main(String[] args) throws IOException {
		MatrixParser teste = new MatrixParser();
		teste.saveOutput();
	}

}
