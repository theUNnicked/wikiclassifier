package pl.gda.pg.eti.kask.kaw.variates;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;
import java.util.TreeSet;

@Deprecated
public class DictionaryTree implements Serializable {

	private static final long serialVersionUID = -1778581097720228638L;
	private DictionaryNode dictionary = null;

	public boolean serializeToFile(String filePath) {
		try {
			FileOutputStream fileOut = new FileOutputStream(filePath);
			ObjectOutputStream out = new ObjectOutputStream(fileOut);
			out.writeObject(dictionary);
			out.close();
			fileOut.close();
			return true;
		} catch (IOException i) {
			i.printStackTrace();
			return false;
		}
	}
	
	public boolean deserializeFromFile(String filePath) {
		try {
			dictionary = null;
			FileInputStream fileIn = new FileInputStream(filePath);
			return deserializeFromFile(fileIn);
		} catch (IOException i) {
			i.printStackTrace();
			return false;
		}
	}

	public boolean deserializeFromFile(InputStream fileIn) {
		try {
			dictionary = null;
			ObjectInputStream in = new ObjectInputStream(fileIn);
			dictionary = (DictionaryNode) in.readObject();
			in.close();
			fileIn.close();
		} catch (IOException i) {
			i.printStackTrace();
			return false;
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
			return false;
		}
		return true;
	}

	public boolean loadDictionaryFromFile(String file) {
		dictionary = new DictionaryNode(3);
		TreeSet<String> fileDictionary;
		try {
			fileDictionary = loadAndParseWordFile(file);
		} catch (IOException e) {
			e.printStackTrace();
			return false;
		}
		System.out.println("Przetworzono plik wejsciowy");
		BufferedWriter writer;
		try {
			writer = new BufferedWriter(new FileWriter("testWords.txt"));
			for (String unique : fileDictionary) {
				writer.write(unique);
				writer.newLine();
			}
			writer.close();
		} catch (IOException e) {
			e.printStackTrace();
			return false;
		}
		System.out.println("Zapisano do pliku testWords.txt");
		int count = 0;
		for (String line : fileDictionary) {
			String[] lineArray = line.split("\t");
			dictionary.insertWordWithLexem(lineArray[0], lineArray[1]);
			count++;
			if(count % 100000 == 0) {
				System.out.println("Przetworzono " + count + "/" + fileDictionary.size() + " słów.");		
			}
		}
		System.out.println("Utworzono slownik");
		return true;
	}

	public String getWordLexem(String word) {
		word = word.toLowerCase();
		String lexem = dictionary.getLexem(word);
		if (lexem == null) {
			return word;
		} else {
			return lexem;
		}
	}

	private TreeSet<String> loadAndParseWordFile(String file) throws IOException {
		BufferedReader reader;
		reader = new BufferedReader(new FileReader(file));
		Set<String> lines = new HashSet<String>(10000000);
		String line;
		while ((line = reader.readLine()) != null) {
			String newLine = "";
			String[] lineArray = line.split("\t");
			if (lineArray.length >= 2) {
				newLine = lineArray[0] + "\t" + lineArray[1];
				newLine = newLine.toLowerCase();
				lines.add(newLine);
			}
		}
		reader.close();
		TreeSet<String> orderedSet = new TreeSet<String>();
		orderedSet.addAll(lines);
		return orderedSet;
	}
}
