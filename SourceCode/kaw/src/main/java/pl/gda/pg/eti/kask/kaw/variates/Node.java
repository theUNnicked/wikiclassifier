package pl.gda.pg.eti.kask.kaw.variates;

import java.util.HashMap;

public class Node {
	public String baseWord = "";
	public HashMap<Character, Node> leaves = new HashMap<Character, Node>();
}
