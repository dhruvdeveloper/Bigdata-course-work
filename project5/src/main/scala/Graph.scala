import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import scala.collection.mutable.ListBuffer
 

object Graph {
  val start_id = 14701391
  val max_int = Int.MaxValue
  val iterations = 5

 

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Graph").setMaster("local[2]")
    val sc = new SparkContext(conf)
    

 

    val values_graph = sc.textFile(args(0)).map(line => {
      val scanner = line.split(",")
      (scanner(1).toInt, scanner(0).toInt)
    }) // create a graph edge (i,j), where i follows j

 

    var R = values_graph.groupByKey().map {
      case (keyval, valueval) =>
        if (keyval == start_id || keyval == 1) {
          (keyval, 0)
        } else {
          (keyval, max_int)
        }
    }

 

    // starting point has distance 0, while the others max_int

 

    //for (i <- 0 to iterations) {
    
     var CC = R.join(values_graph).flatMap {
        case(keyval,a) =>
          var b = new ListBuffer[(Int, (Int, Int))]
          if(a._1 < max_int){
            b.append((a._2, (a._1 + 1, 777777)))
            b.append((keyval, (a._1, a._2)))
          }
          else{
            b.append((keyval, (a._1, a._2)))
          }
          b
	    }
     
     
     
     var abc = new ListBuffer[(Int, Int)]
     
     var EE = CC.groupByKey().mapValues{
       sq => sq.min
     }
     
     var EE1 = EE.map(x => {
       if(x._2._1 < 222){
         abc.append((x._1, x._2._1))
       }
     })
     
     
    
     
//     var DD = CC.map(line => {
//       var b = new ListBuffer[(Int, (Int, Int))]
//       if(line._2._1 < max_int)
//            b.append((a._2, (a._1 + 1, 777777)))
//            //b.append((keyval, (a._1, a._2))) 
//          else{
//            b.append((keyval, (a._1, a._2))) 
//          }
//          b
//     })
     
     
    //}
    //var r1 = R.join(values_graph)
 
   //oo1.collect().foreach(println)
   EE.collect().foreach(println)
  }
}
