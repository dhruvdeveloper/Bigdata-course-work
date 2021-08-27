import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object Twitter {
	def main(args: Array[String]): Unit = {
		val condata = new SparkConf()
    		val scanfile = new SparkContext(condata)
		var datafetch = scanfile.textFile(args(0)).map(line => {
  	   		 val dat = line.split(",")
    			 (dat(1).toDouble,dat(0).toDouble)
      		}).groupByKey().mapValues(sq => (sq.size)).map{
			case(a, b) => 
			(b, 1)
		}.groupByKey().mapValues(sq => (sq.size)).collect.sorted.foreach(println)
  	}
}
