package Garph_Demo

import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx.{ Edge, Graph }

case class User(name: String, age: Int, inDeg: Int, outDeg: Int)

/**
 * Employee relationship analysis with GraphX.
 */
object assginment {
	def main(args: Array[String]): Unit = {
		// vertex format: vertex_id, data
		val vertexArray = Array(
			(1L, ("Alice", 28)),
			(2L, ("Bob", 27)),
			(3L, ("Charlie", 35)),
			(4L, ("David", 50)),
			(5L, ("Edward", 35)),
			(6L, ("Fran", 58)))

		// edge format: from_vertex_id, to_vertex_id, data
		val edgeArray = Array(
			Edge(2L, 1L,7),
			Edge(2L, 4L, 2),
			Edge(3L, 2L, 4),
			Edge(6L, 3L, 3),
			Edge(4L, 1L, 1),
			Edge(5L, 2L, 2),
			Edge(5L, 3L, 8),
			Edge(5L, 6L, 3))

		val sc = new SparkContext(new SparkConf().setAppName("EmployeeRelationshipJob").setMaster("local[*]"))

		val vertexRDD: RDD[(Long, (String, Int))] = sc.parallelize(vertexArray)

		val edgeRDD: RDD[Edge[Int]] = sc.parallelize(edgeArray)

		val graph: Graph[(String, Int), Int] = Graph(vertexRDD, edgeRDD)

		// Vanilla query
		println(">>> display the name of the users older than 30 years old.")
				
	graph.vertices.filter(_._2._2 > 30).foreach(v => println(s"${v._2._1} is ${v._2._2}"))

		// Connection analysis
		println(">>> Create a user Graph) -> ")
		// Create a user Graph
val initialUserGraph: Graph[User, Int] = graph.mapVertices {
case (id, (name, age)) => User(name, age, 0, 0)
}

		println(">>> Display who likes who (if the edge value is greater than 5) -> ")
          graph.triplets.filter(_.attr > 5).foreach(t =>
println(s"${t.srcAttr._1} likes ${t.dstAttr._1}"))

// Fill in the degree information
/**
* def outerJoinVertices[U, VD2](other: RDD[(VertexID, U)])
* (mapFunc: (VertexID, VD, Option[U]) => VD2)
* : Graph[VD2, ED]
*/


val userGraph = initialUserGraph.outerJoinVertices(initialUserGraph.inDegrees) {
case (id, u, inDegOpt) => User(u.name, u.age, inDegOpt.getOrElse(0), u.outDeg)
}.outerJoinVertices(initialUserGraph.outDegrees) {
case (id, u, outDegOpt) => User(u.name, u.age, u.inDeg, outDegOpt.getOrElse(0))
}



		println(">>> Technical Mentoring Analysis -> ")
		graph.triplets.filter(_.attr.equals("Technical Mentor")).collect()
			.foreach { item => println("... " + item.srcAttr._1 + " mentoring " + item.dstAttr._1) }
		
		println(">>>>Display who follows who (through the edges direction) ->")
		graph.triplets.foreach(t => println(s"${t.srcAttr._1} follows ${t.dstAttr._1}"))
	}
}