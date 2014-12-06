import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

object ShortestPath {
        def main(args: Array[String]) {
                //Spark Context
                val tmpSC = new SparkContext

                //Read the file describing the graph. Hard-coded to "graph.txt"
                //File Format should be:
                //SrcVertex     DestVertex      EdgeWeight
                var graphFile = tmpSC.textFile("/user/ubuntu/GoogleGraphShortestPath/binaryTree.txt")

                //Extract edge and vertex information from file. Note that this file cannot contain comments/empty lines
                var tmpEdges = graphFile.map(s => Edge(s.split("\\s+")(0).toLong, s.split("\\s+")(1).toLong, s.split("\\s+")(2).toInt))

                //Get all source vertices from 1st column
                var tmpSrcVertices = graphFile.map(s => (s.split("\\s+")(0).toLong, 1))
                //Get destination vertices from 2nd column
                var tmpDstVertices = graphFile.map(s => (s.split("\\s+")(1).toLong, 1))

                //Combine above 2 RDDs to remove duplicate vertices
                var tmpVertices = tmpSrcVertices.union(tmpDstVertices).reduceByKey( (value1, value2) => 1 )

                //Create graph using the edge, vertex RDDs
                var tmpGraph = Graph(tmpVertices, tmpEdges)

                //Initialize graph with vertex weight
                var initGraph = tmpGraph.mapVertices((vid, weight) => if (vid==1) 0 else (Int.MaxValue/2))

                //Pregel superstep: send message to destination vertex in map function and combine all recieved messages in reduce function
                var superStep0: RDD[(VertexId, Int)] = initGraph.mapReduceTriplets(et => Iterator((et.dstId, (et.srcAttr+et.attr))), (value1, value2) => math.min(value1,value2))

                //Count total number of messages
                var messageCount: Long = superStep0.count()

                var prevGraph: Graph[Int, Int] = null
                var innerCount: Int = 0;
                var outerCount: Int = 0;


                initGraph.vertices.saveAsObjectFile("/user/ubuntu/GoogleGraphShortestPath/verticesObject_0")
                initGraph.edges.saveAsObjectFile("/user/ubuntu/GoogleGraphShortestPath/edgesObject_0")

                //Iterator
                while ((messageCount > 0) &&  (outerCount < 1000)) { //(outerCount < (Int.MaxValue/2))) {

                        val iterVertices = tmpSC.objectFile[(VertexId, Int)]("/user/ubuntu/GoogleGraphShortestPath/verticesObject_"+outerCount)
                        val iterEdges = tmpSC.objectFile[Edge[Int]]("/user/ubuntu/GoogleGraphShortestPath/edgesObject_"+outerCount)
                        var iterGraph = Graph(iterVertices, iterEdges)

                        innerCount = 0

                        while ((messageCount > 0) && (innerCount < 50)) {

                                //Update vertex value if recieved message value is lesser than original
                                //Variable newVertices only contains the set of vertices which recieved atleast 1 message
                                val newVertices = iterGraph.vertices.innerJoin(superStep0)((Vid, oldattr, newattr) => if (newattr < oldattr) newattr else oldattr)
                                newVertices.cache()
                                //println("*********Message*************InnerJoin")

                                //Re-create the graph with new values
                                prevGraph = iterGraph
                                iterGraph = iterGraph.outerJoinVertices(newVertices){ (Vid, old, newOpt) => newOpt.getOrElse(old)}
                                iterGraph.cache()
                                //println("*********Message************OuterJoin")

                                //Re-run Pregel superstep restricting vertex set to newVertices
                                val oldSuperStep0 = superStep0
                                superStep0 = iterGraph.mapReduceTriplets(et => Iterator((et.dstId, (et.srcAttr+et.attr))), (value1, value2) => math.min(value1,value2), Option(newVertices, EdgeDirection.Out))
                                superStep0.cache()
                                //println("*********Message***********SuperStep")

                                //Count total number of messages
                                messageCount = superStep0.count()

                                innerCount += 1;
                                //println("Message count is: "+messageCount)
                                //println("Iteration updated")
                                //println("Iteration count is: "+(innerCount+outerCount))


                                newVertices.unpersist(blocking=false)
                                prevGraph.unpersistVertices(blocking=false)
                                prevGraph.edges.unpersist(blocking=false)
                                oldSuperStep0.unpersist(blocking=false)
                        }

                        //Print answer
                        outerCount += 50
                        //println("Outer Iteration count is: "+outerCount)
                        iterGraph.vertices.saveAsObjectFile("/user/ubuntu/GoogleGraphShortestPath/verticesObject_"+outerCount)
                        iterGraph.edges.saveAsObjectFile("/user/ubuntu/GoogleGraphShortestPath/edgesObject_"+outerCount)
                        iterGraph.vertices.unpersist(blocking=false)
                        iterGraph.edges.unpersist(blocking=false)
                        iterVertices.unpersist(blocking=false)
                        iterEdges.unpersist(blocking=false)
                }
        }
}
