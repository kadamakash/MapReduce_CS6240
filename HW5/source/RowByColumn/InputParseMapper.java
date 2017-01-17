package hw5;

import java.io.IOException;  
import java.io.StringReader;
import java.net.URLDecoder;

import java.util.regex.Matcher;
import java.util.regex.Pattern;


import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.xml.sax.Attributes;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;
import org.xml.sax.helpers.DefaultHandler; 

// The inputParseMapper reads the bz2 compressed zip file and emits the 
// page name with the outlink list. It also keeps the counter for total
// page nodes and the dangling nodes
public class InputParseMapper extends Mapper<Object, Text, Text, Text> {

	//private List<String> linkPageNames;
	//private SAXParserFactory spf;
	//private SAXParser saxParser;
	private XMLReader xmlReader;
	private static WikiParser userhandler;
	private static Pattern namePattern;
	private static Pattern linkPattern;

	public static final String PAGE_COUNTER_GROUP = "Total";
	static {
		// Keep only html pages not containing tilde (~).
		namePattern = Pattern.compile("^([^~]+)$");
		// Keep only html filenames ending relative paths and not containing tilde (~).
		linkPattern = Pattern.compile("^\\..*/([^~]+)\\.html$");
		
	}

	@Override
	protected void setup(Context context)
			throws IOException, InterruptedException {

		try{
            // create a SAX parser and xml reader to read the line
            SAXParserFactory factory = SAXParserFactory.newInstance();
            factory.setFeature("http://apache.org/xml/features/nonvalidating/load-external-dtd", false);
            SAXParser saxParser = factory.newSAXParser();
            xmlReader = saxParser.getXMLReader();
            userhandler=new WikiParser("");
            xmlReader.setContentHandler(userhandler);
        }catch(Exception e){
                e.printStackTrace();
        }
		
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
		
		//StringBuilder outlinks = new StringBuilder();
		
		if (!matcher.find()) {
			// Skip this html file, name contains (~).
			return;
		}

		// Parse page and fill list of linked pages.
		//linkPageNames.clear();
		try {
			userhandler.linkPageNames = "";
			xmlReader.parse(new InputSource(new StringReader(html)));
			String l = userhandler.linkPageNames;
			context.getCounter(PAGE_COUNTER_GROUP, "Total").increment(1);
			if(l.equals("")){
                context.getCounter(PAGE_COUNTER_GROUP, "Dangling").increment(1);
                context.write(new Text(pageName),new Text(l));
            }
            else{
                //System.out.println(s);
                context.write(new Text(pageName),new Text(l.substring(0,l.lastIndexOf(",")))); // just emit
            }
		} catch (Exception e) {
			// Discard ill-formatted pages.
			return;
		}

		// Occasionally print the page and its links.
		/*if (Math.random() < .01f) {
			System.out.println(pageName + " - " + linkPageNames);
		}*/
		//outlinks.append(pageName);
		//outlinks.append("->");
		/*context.getCounter(PAGE_COUNTER_GROUP, "Total").increment(1);
		for(String l : linkPageNames){
			//outlinks.append(l);
			//outlinks.append(",");
			if(l.equals("")){
				context.getCounter(PAGE_COUNTER_GROUP, "Dangling").increment(1);
                context.write(new Text(pageName),new Text(l));
			}
			else{
				context.write(new Text(pageName),new Text(l));
			}
		}*/
		
		//if((outlinks.charAt(outlinks.length() - 1)) == ','){
		//outlinks.deleteCharAt(outlinks.length() - 1); 
		//}
		//outlinks.append("->");
		//outlinks.append("0.0");
		
		//context.getCounter(CounterGroup.NumberOfPages).increment(1L);
		//context.write(new Text(outlinks.toString()), NullWritable.get());
		//System.out.println(outlinks);
	}

	/*@Override
	protected void cleanup(Context context)
			throws IOException, InterruptedException {
		
	}*/

	/** Parses a Wikipage, finding links inside bodyContent div element. */
	private static class WikiParser extends DefaultHandler {


		/** List of linked pages; filled by parser. */
		private String linkPageNames;
		/** Nesting depth inside bodyContent div element. */
		private int count = 0;

		public WikiParser(String linkPageNames) {
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
					linkPageNames += matcher.group(1)+",";
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

