package edu.hanyang.submit;

import java.util.*;

import edu.hanyang.indexer.Tokenizer;
import org.apache.lucene.analysis.core.SimpleAnalyzer;
import org.tartarus.snowball.ext.PorterStemmer;


public class TinySETokenizer implements Tokenizer {
	private SimpleAnalyzer analyzer;
	private PorterStemmer stemmer;
	public void setup() {
		analyzer = new SimpleAnalyzer();
		stemmer = new PorterStemmer();
	}

	public List<String> split(String text) {
		List<String> finishedList = new ArrayList<>();
		String tokenizedText = "";
		analyzer.tokenStream(text,tokenizedText);
		System.out.println(tokenizedText);

		return null;
	}

	public void clean() {
		analyzer.close();
	}

}