package edu.hanyang.submit;

import java.io.IOException;
import java.util.*;

import edu.hanyang.indexer.Tokenizer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.core.SimpleAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.tartarus.snowball.ext.PorterStemmer;


public class TinySETokenizer implements Tokenizer {
	private SimpleAnalyzer analyzer;
	private PorterStemmer stemmer;

	/*Creating the analyzer and stemmer*/
	public void setup() {
		analyzer = new SimpleAnalyzer();
		stemmer = new PorterStemmer();
	}

	/*First we split the text up into words and put them separate in an arraylist.
	* We do this using the SimpleAnalyzers tokenstream and using the CharTermAttribute.
	* After this we put every item in the arraylist though the PorterStemmer and remove the old item and
	* put the new item into the list with the same index as the old item.*/

	/*We chose to keep the constructors for the TokenStream and CharTermAtrribute out of the setup and clean function*/
	public List<String> split(String text){
		List<String> finishedList = new ArrayList<>();

		/*Tokenizes the text to create a list of all words to be stemmed after separation*/
		TokenStream tokenStream = analyzer.tokenStream("fieldName", text);
		CharTermAttribute attribute = tokenStream.addAttribute(CharTermAttribute.class);
		try {
			tokenStream.reset();
			while(tokenStream.incrementToken()) {
				finishedList.add(attribute.toString());
			}
			tokenStream.close();
		} catch (IOException e) {
			e.printStackTrace();
		}

		/*Stemmes every String in the arraylist and replaces the original string with the stemmed string*/
		for (int i = 0; i<finishedList.size();i++){
			stemmer.setCurrent(finishedList.get(i));
			stemmer.stem();
			finishedList.remove(i);
			finishedList.add(i,stemmer.getCurrent());
		}
		return finishedList;
	}
	/*Closes the analyzer to free the resources used by the analyzer*/
	public void clean() {
		analyzer.close();
	}

}