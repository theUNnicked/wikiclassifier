package pl.gda.pg.eti.kask.kaw.variates;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;

public class Dictionary {

	private HashMap<Character, Node> roots = new HashMap<Character, Node>();

	public String getWordLexeme(String word) {
		if (roots.containsKey(word.charAt(0))) {
			if (word.length() == 1) {
				if (roots.get(word.charAt(0)).baseWord == "") {
					return word;
				} else {
					return roots.get(word.charAt(0)).baseWord;
				}
			}
			String returnedWord = search(word.substring(1), roots.get(word.charAt(0)));
			if (returnedWord != "") {
				return returnedWord;
			}
		}
		return word;
	}

	public void insertWordLexeme(String word, String lexim) {
		if (!roots.containsKey(word.charAt(0))) {
			roots.put(word.charAt(0), new Node());
		}
		if (word.length() == 1) {
			roots.get(word.charAt(0)).baseWord = lexim;
		} else {
			insert(word.substring(1), roots.get(word.charAt(0)), lexim);
		}
	}

	private String search(String word, Node searchedNode) {
		if (word.length() == 0) {
			return searchedNode.baseWord;
		} else {
			Node node = searchedNode.leaves.get(word.charAt(0));
			if (node != null) {
				return search(word.substring(1), node);
			}
		}
		return "";
	}

	private void insert(String word, Node insertNode, String wordToInsert) {
		final Node nextChild;
		if (insertNode.leaves.containsKey(word.charAt(0))) {
			nextChild = insertNode.leaves.get(word.charAt(0));
		} else {
			nextChild = new Node();
			insertNode.leaves.put(word.charAt(0), nextChild);
		}
		if (word.length() == 1) {
			nextChild.baseWord = wordToInsert;
			return;
		} else {
			insert(word.substring(1), nextChild, wordToInsert);
		}
	}
	
	public boolean loadDictionary(InputStream fileIn) {
		BufferedReader reader;
		try {
			reader = new BufferedReader(new InputStreamReader(fileIn));
			String line;
			while ((line = reader.readLine()) != null) {
				String[] lineArray = line.split("\t");
				if (lineArray.length >= 2) {
					insertWordLexeme(lineArray[0], lineArray[1]);
				}
			}
			reader.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
			return false;
		} catch (IOException e) {
			e.printStackTrace();
			return false;
		}
		return true;
	}
}
