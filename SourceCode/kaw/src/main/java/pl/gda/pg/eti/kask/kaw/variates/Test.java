package pl.gda.pg.eti.kask.kaw.variates;

public class Test {
	public static void main(String[] args) {
		DictionaryTree dictionary = new DictionaryTree();
		dictionary.loadDictionaryFromFile("sgjp.txt");
		System.out.println(dictionary.getWordLexem("addycje"));
		System.out.println(dictionary.getWordLexem("addycyjnością"));
		System.out.println(dictionary.getWordLexem("addytywności"));
		System.out.println(dictionary.getWordLexem("aa"));
		dictionary.serializeToFile("dictionaryTest.txt");
		dictionary.deserializeFromFile("dictionaryTest.txt");
		System.out.println(dictionary.getWordLexem("addycje"));
		System.out.println(dictionary.getWordLexem("addycyjnością"));
		System.out.println(dictionary.getWordLexem("addytywności"));
		System.out.println(dictionary.getWordLexem("aa"));
	}
}
