package pl.gda.pg.eti.kask.kaw.io;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.ArrayList;
import java.util.List;

public class SplitScanner {

	private static final int BLOCK_SIZE = 256;
	private InputStream in;
	private boolean nextLine = false;
	private boolean endOfFile = false;
	private String bufferedText = "";
	private String nextLineString = "";
	private List<String> preparedList = new LinkedList<String>();

	public SplitScanner(InputStream in) {
		this.in = in;
	}

	/**
	 * sprawdza czy da sie wyciagnac kolejny element, jezeli koniec liniki
	 * zwraca false
	 * 
	 * @throws IOException
	 */
	public boolean hasNext() throws IOException {
		if (preparedList.size() > 0) {
			return true;
		}
		if (endOfFile == true || nextLine == true) {
			return false;
		}
		return readNextChunk();
	}

	/**
	 * zwraca kolejny element wewnatrz liniki
	 * 
	 * @throws IOException
	 */
	public String takeNext() throws IOException {
		if (preparedList.size() > 0) {
			String element = preparedList.get(0);
			preparedList.remove(0);
			return element;
		}
		if (endOfFile == true || nextLine == true) {
			return "";
		}
		if (readNextChunk()) {
			String element = preparedList.get(0);
			preparedList.remove(0);
			return element;
		} else {
			return "";
		}
	}

	/**
	 * sprawdza czy istnieje kolejna linika, jezeli koniec pliku to zwraca false
	 * jezeli hasNext zwróciłoby true, hasNextLine takze zwróci true
	 * 
	 * @throws IOException
	 */
	public boolean hasNextLine() throws IOException {
		if (endOfFile && preparedList.size() == 0) {
			return false;
		}
		if (nextLine == false && preparedList.size() == 0) {
			return readNextChunk();
		}
		return true;
	}

	/**
	 * ustawia flage w hasNext na true i przechodzi do wczytywania elementow z
	 * kolejnej linii
	 */
	public void takeNextLine() {
		if (nextLine) {
			nextLine = false;
			// bufferedText = nextLineString;
			List<String> tempList = new ArrayList<String>(Arrays.asList(nextLineString.split("\\t")));
			if (tempList.size() > 1) {
				int positionToRemove = tempList.size() - 1;
				bufferedText = tempList.remove(positionToRemove);
				preparedList.addAll(tempList);
			} else {
				bufferedText = tempList.get(0);
			}
			nextLineString = "";
		} else {
			// TODO:
		}
	}

	private boolean readNextChunk() throws IOException {
		while (true) {
			byte[] byteArray = new byte[BLOCK_SIZE];
			int sizeRead = in.read(byteArray, 0, BLOCK_SIZE);
			if (sizeRead == -1) {
				preparedList.add(bufferedText);
				bufferedText = "";
				endOfFile = true;
				if (preparedList.size() > 0) {
					return true;
				} else {
					return false;
				}
			}
			String temp = bufferedText + new String(byteArray, "UTF-8");
			int lineSplitLocation = temp.indexOf('\n');
			if (lineSplitLocation != -1) {
				nextLine = true;
				if (temp.length() > lineSplitLocation + 1) {
					nextLineString = temp.substring(lineSplitLocation + 1);
				}
				if (lineSplitLocation > 0) {
					bufferedText = temp.substring(0, lineSplitLocation);
					preparedList.addAll(Arrays.asList(bufferedText.split("\\t")));
				}
				return true;
			} else {
				List<String> tempList = new ArrayList<String>(Arrays.asList(temp.split("\\t")));
				if (tempList.size() > 1) {
					int positionToRemove = tempList.size() - 1;
					bufferedText = tempList.remove(positionToRemove);
					preparedList.addAll(tempList);
					return true;
				} else {
					bufferedText = tempList.get(0);
				}
			}
		}
	}
}