package pl.gda.pg.eti.kask.kaw.variates

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class DictionaryNode implements Serializable{
	
	private static final long serialVersionUID = -1335933171986936438L;
	private String key;
	private String value;
	private List<DictionaryNode> leaves;
	private int depth;
	private int maxDepth;
	
	DictionaryNode(int maxDepth) {
		this.depth = 0;
		this.maxDepth = maxDepth;
		this.key = null;
		this.value = null;
		this.leaves = new ArrayList<DictionaryNode>();
	}
	
	DictionaryNode(int maxDepth, int depth) {
		this.depth = depth;
		this.maxDepth = maxDepth;
		this.key = null;
		this.value = null;
		this.leaves = new ArrayList<DictionaryNode>();
	}
	
	public void insertWordWithLexem(String word, String lexem) {
		if(depth == word.length()) {
			value = lexem;
			key = word;
			return;
		}
		if(depth == maxDepth) {
			value = lexem;
			key = word;
		}
		else {
			for(DictionaryNode leaf : leaves) {
				if(leaf.getKey().equals(word.substring(0,leaf.getDepth()))) {
					leaf.insertWordWithLexem(word, lexem);
					return;
				}
			}
			DictionaryNode leaf = new DictionaryNode(this.maxDepth, depth+1);
			leaf.setKey(word.substring(0,leaf.getDepth()));
			leaf.insertWordWithLexem(word, lexem);
			leaves.add(leaf);
		}
	}
	
	public String getLexem(String word) {
		if(leaves.isEmpty() || word.length() == depth) {
			if(word.equals(key)) {
				return this.value;
			}
			return null;
		}
		else {
			if(word.substring(0, depth).equals(key) || depth == 0) {
				for(DictionaryNode leaf: leaves) {
					String lexem = leaf.getLexem(word);
					if(lexem != null) {
						return lexem;
					}
				}
				return null;
			}
			return null;
		}
	}
	
	public String getKey() {
		return this.key;
	}
	
	public void setKey(String key) {
		this.key = key;
	}
	
	public String getValue() {
		return this.value;
	}
	
	public void setValue(String value) {
		this.value = value;
	}
	
	public int getDepth() {
		return this.depth;
	}
}
