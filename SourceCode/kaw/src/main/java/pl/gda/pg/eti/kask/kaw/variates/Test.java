package pl.gda.pg.eti.kask.kaw.variates

public class Test {
	public static void main(String[] args) {
		DictionaryTree dictionary = new DictionaryTree();
		dictionary.loadDictionaryFromFile("sgjp.txt");
		System.out.println(dictionary.getWordLexeme("addycje"));
		System.out.println(dictionary.getWordLexeme("addycyjnością"));
		System.out.println(dictionary.getWordLexeme("addytywności"));
		System.out.println(dictionary.getWordLexeme("aa"));
		dictionary.serializeToFile("dictionaryTest.txt");
		dictionary.deserializeFromFile("dictionaryTest.txt");
		System.out.println(dictionary.getWordLexeme("addycje"));
		System.out.println(dictionary.getWordLexeme("addycyjnością"));
		System.out.println(dictionary.getWordLexeme("addytywności"));
		System.out.println(dictionary.getWordLexeme("aa"));
	}
}
