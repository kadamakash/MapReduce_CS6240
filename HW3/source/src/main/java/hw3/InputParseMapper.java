package hw3;

import java.io.IOException;
import java.io.StringReader;
import java.net.URLDecoder;
import java.util.LinkedList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.xml.sax.Attributes;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.SAXNotRecognizedException;
import org.xml.sax.SAXNotSupportedException;
import org.xml.sax.XMLReader;
import org.xml.sax.helpers.DefaultHandler; 

public class InputParseMapper extends Mapper<Object, Text, Text, NullWritable> {

	private List<String> linkPageNames;
	private SAXParserFactory spf;
	private SAXParser saxParser;
	private XMLReader xmlReader;

	private static Pattern namePattern;
	private static Pattern linkPattern;
	static {
		// Keep only html pages not containing tilde (~).
		namePattern = Pattern.compile("^([^~]+)$");
		// Keep only html filenames ending relative paths and not containing tilde (~).
		linkPattern = Pattern.compile("^\\..*/([^~]+)\\.html$");
	}

	@Override
	protected void setup(Context context)
			throws IOException, InterruptedException {

		// Configure parser
		spf = SAXParserFactory.newInstance();
		try {
			spf.setFeature("http://apache.org/xml/features/nonvalidating/load-external-dtd", false);
		} catch (SAXNotRecognizedException | SAXNotSupportedException
				| ParserConfigurationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		try {
			saxParser = spf.newSAXParser();
		} catch (ParserConfigurationException | SAXException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		try {
			xmlReader = saxParser.getXMLReader();
		} catch (SAXException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		// Parser fills this list with linked page names.
		linkPageNames = new LinkedList<>();
		xmlReader.setContentHandler(new WikiParser(linkPageNames));

	}
	
	/* map processes the linkPage LinkedList and creates a list for the job2 which
	 * is of the format " PageName->(list of outlinks seperated by ',')->PageRank "
	 * StringBuilder is used to achieve the format
	 * Map emits key as the line mentioned in the above format and value is Null
	 * */

	@Override
	protected void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		// Each line formatted as (Wiki-page-name:Wiki-page-html).
		String line = value.toString();
		int delimLoc = line.indexOf(':');
		String pageName = line.substring(0, delimLoc);
		String html = line.substring(delimLoc + 1);
		Matcher matcher = namePattern.matcher(pageName);
		
		StringBuilder outlinks = new StringBuilder();
		
		if (!matcher.find()) {
			// Skip this html file, name contains (~).
			return;
		}

		// Parse page and fill list of linked pages.
		linkPageNames.clear();
		try {
			xmlReader.parse(new InputSource(new StringReader(html)));
		} catch (Exception e) {
			// Discard ill-formatted pages.
			return;
		}

		// Occasionally print the page and its links.
		/*if (Math.random() < .01f) {
			System.out.println(pageName + " - " + linkPageNames);
		}*/
		outlinks.append(pageName);
		outlinks.append("->");
		
		for(String l : linkPageNames){
			outlinks.append(l);
			outlinks.append(",");
		}
		
		if((outlinks.charAt(outlinks.length() - 1)) == ','){
		outlinks.deleteCharAt(outlinks.length() - 1); 
		}
		outlinks.append("->");
		outlinks.append("0.0");
		
		context.getCounter(CounterGroup.NumberOfPages).increment(1L);
		context.write(new Text(outlinks.toString()), NullWritable.get());
		//System.out.println(outlinks);
	}

	/*@Override
	protected void cleanup(Context context)
			throws IOException, InterruptedException {
		
	}*/

	/** Parses a Wikipage, finding links inside bodyContent div element. */
	private static class WikiParser extends DefaultHandler {


		/** List of linked pages; filled by parser. */
		private List<String> linkPageNames;
		/** Nesting depth inside bodyContent div element. */
		private int count = 0;

		public WikiParser(List<String> linkPageNames) {
			super();
			this.linkPageNames = linkPageNames;
		}

		@Override
		public void startElement(String uri, String localName, String qName, Attributes attributes) throws SAXException {
			super.startElement(uri, localName, qName, attributes);
			if ("div".equalsIgnoreCase(qName) && "bodyContent".equalsIgnoreCase(attributes.getValue("id")) && count == 0) {
				// Beginning of bodyContent div element.
				count = 1;
			} else if (count > 0 && "a".equalsIgnoreCase(qName)) {
				// Anchor tag inside bodyContent div element.
				count++;
				String link = attributes.getValue("href");
				if (link == null) {
					return;
				}
				try {
					// Decode escaped characters in URL.
					link = URLDecoder.decode(link, "UTF-8");
				} catch (Exception e) {
					// Wiki-weirdness; use link as is.
				}
				// Keep only html filenames ending relative paths and not containing tilde (~).
				Matcher matcher = linkPattern.matcher(link);
				if (matcher.find()) {
					linkPageNames.add(matcher.group(1));
				}
			} else if (count > 0) {
				// Other element inside bodyContent div.
				count++;
			}
		}

		@Override
		public void endElement(String uri, String localName, String qName) throws SAXException {
			super.endElement(uri, localName, qName);
			if (count > 0) {
				// End of element inside bodyContent div.
				count--;
			}
		}
	}


}

