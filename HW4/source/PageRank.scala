import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

object PageRank {
	def main(args: Array[String]){

		try{
		val conf = new SparkConf().setAppName("Page Rank in Scala").setMaster("local")
		val sc = new SparkContext(conf)
		val alpha = 0.15	
		
		val input = args(0)
		val outputPath = args(1)
		val inputGraph = sc.textFile(input)
							.map(line => WikiParser.lineGenerator(line)) // parse each line from BZ2 using WikiParser
							.filter(line => !line.contains("invalid"))   // filter all the lines which are invalid
							.map(line => line.split("->"))               // gets the pageName and outLinks as 2 parts
							.map(line => {if(line.length > 1) {          // if the node is a dangling node then emit pageName with empty list 
							  (line(0), line(1).split("~").toList)       // or else emit the pageName with the out link list
							  } else {(line(0), List[String]())
							    }})
							.keyBy(k => k._1)                            // generates a key value pair with pageName as key and (pageName, outlinklist) as value
							.mapValues(line => line._2)                  // keeps only the outlink list as the value in the K,V pair
							

		inputGraph.persist() // caches the RDD in memory 
		val allList = inputGraph.values                                    // considering only the values from the K,V pair
					.flatMap(line => line)                     // creats a flatmap
					.keyBy(k => k)				   // K,V pair with the pageName as key 
					.reduceByKey((a,b) => a)		   // reduce it by key
					.map(line => (line._1, List[String]()))	   // maps key, empty list of String
						

		// pageName and linklist union is taken to get the complete list inclding the dangling nodes
		var finalLinks = inputGraph.union(allList).reduceByKey((a,b) => a.++(b))  
		
		// get the total pageCount 
		val pageCount = finalLinks.count		
		// println("page count=" + pageCount)
		// assigining the initial pageRank values for all Pages in the list 
		var pageRankList = finalLinks.mapValues(l => 1.0 / pageCount)
		
		// computing the pageRank for 10 iterations	
		pageRankList.persist()	
		for(itr <- 1 to 10){
			try{
			val danglingFactor = sc.accumulator(0.0)   // danglingFactor accumulator
			var prList = finalLinks.join(pageRankList) // join on the finalList with the pageRank values
					       .values             // to operate only on the values of the K,V pair
					       .flatMap{
			  case(links, rank) => {                   // for all the (outlinks, pageRank) pair 
			    val count = links.size                 // gets the outlinks size
			    if(count != 0){                        // if not 0 calculate P(m)/C(m)
			      links.map(l => (l, rank/count))
			    }else{
			      danglingFactor += rank               // if size is 0 means its a dangling node, so increment the danglingFactor by the rank value and return empty List
			      List()
			    }
			  }
			}
			prList.count
			val df = danglingFactor.value
			//println("prList Count =" + prList.count)
			pageRankList = prList.reduceByKey(_ + _)          // reduce by Key and compute the page Rank for all the pages
					     .mapValues[Double](pByC => 0.15 * (1.0 / pageCount) + 0.85 * (df / pageCount + pByC))
				
		}catch{
			case e: Exception => println("Exception: " + e);
			}
		}
		
		val top100 = sc.parallelize(pageRankList
					.sortBy(r => r._2, false, 1) // sorting the page rank list based on the page ranks, false indicates descending order
					.repartition(1)		     // repartition all records after sorting
					.take(100))                  // take only top 100
					 		     
		top100.saveAsTextFile(outputPath)     // save the top 100 as Text File
		}
		catch{
		case e: Exception => println("Exception: " + e);
		}
		
	}
}
