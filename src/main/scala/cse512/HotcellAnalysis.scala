package cse512

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame

object HotcellAnalysis {
  Logger.getLogger("org.spark_project").setLevel(Level.WARN)
  Logger.getLogger("org.apache").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)
  Logger.getLogger("com").setLevel(Level.WARN)

def runHotcellAnalysis(spark: SparkSession, pointPath: String): DataFrame =
{
  // Load the original data from a data source
  var pickupInfo = spark.read.format("csv").option("delimiter",";").option("header","false").load(pointPath);
  pickupInfo.createOrReplaceTempView("nyctaxitrips")
  // pickupInfo.show()

  // Assign cell coordinates based on pickup points
  spark.udf.register("CalculateX",(pickupPoint: String)=>((
    HotcellUtils.CalculateCoordinate(pickupPoint, 0)
    )))
  spark.udf.register("CalculateY",(pickupPoint: String)=>((
    HotcellUtils.CalculateCoordinate(pickupPoint, 1)
    )))
  spark.udf.register("CalculateZ",(pickupTime: String)=>((
    HotcellUtils.CalculateCoordinate(pickupTime, 2)
    )))
  pickupInfo = spark.sql("select CalculateX(nyctaxitrips._c5),CalculateY(nyctaxitrips._c5), CalculateZ(nyctaxitrips._c1) from nyctaxitrips")
  var newCoordinateName = Seq("x", "y", "z")
  pickupInfo = pickupInfo.toDF(newCoordinateName:_*)
  pickupInfo.createOrReplaceTempView("pickupInfoView")
  pickupInfo.show()

  // Define the min and max of x, y, z
  val minX = -74.50/HotcellUtils.coordinateStep
  val maxX = -73.70/HotcellUtils.coordinateStep
  val minY = 40.50/HotcellUtils.coordinateStep
  val maxY = 40.90/HotcellUtils.coordinateStep
  val minZ = 1
  val maxZ = 31
  val numCells = (maxX - minX + 1)*(maxY - minY + 1)*(maxZ - minZ + 1)

  pickupInfo = spark.sql(s"select x,y,z from pickupInfoView where x>= ${minX} and x<= ${maxX} and y>= ${minY} and y<= ${maxY} and z>= ${minZ} and z<= ${maxZ} order by z,y,x")
  pickupInfo.createOrReplaceTempView("selectedCellVals")

  pickupInfo = spark.sql("select x, y, z, count(*) as hotCells from selectedCellVals group by z, y, x order by z,y,x").persist()
  pickupInfo.createOrReplaceTempView("selectedCellHotness")
  pickupInfo.show()

  spark.udf.register("squared", (inputX: Int) => (((inputX*inputX).toDouble)))
  val hotcellCalc = spark.sql("select sum(hotcells) as sumHotcells, sum(squared(hotcells)) as sumSqrHotcells from selectedCellHotness")
  hotcellCalc.show()
  
  val mean = (hotcellCalc.first().getLong(0).toDouble / numCells.toDouble).toDouble
  println(s"Mean is ${mean}")

  val sumSqrHotcells = (hotcellCalc.first().getDouble(1)).toDouble
  val standardDeviation = math.sqrt((sumSqrHotcells.toDouble / numCells.toDouble) - (mean.toDouble * mean.toDouble )).toDouble
  println(s"SD is ${standardDeviation}")

  // Weights calculation
  spark.udf.register("neighbourhood", (inputX: Int, inputY: Int, inputZ: Int, minX: Int, maxX: Int, minY: Int, maxY: Int, minZ: Int, maxZ: Int) => ((HotcellUtils.calcNeighborhood(inputX, inputY, inputZ, minX, minY, minZ, maxX, maxY, maxZ))))
  
  // Spatial Selfjoin
  val neighbourhoodCells = spark.sql(s"select neighbourhood(hc1.x, hc1.y, hc1.z, ${minX}, ${maxX}, ${minY}, ${maxY}, ${minZ},${maxZ}) as spatialWeightSum,"
  		+ "hc1.x as x, hc1.y as y, hc1.z as z, sum(hc2.hotCells) as sumHotCells from selectedCellHotness as hc1, selectedCellHotness as hc2 "
  		+ "where (hc2.x = hc1.x+1 or hc2.x = hc1.x or hc2.x = hc1.x-1) " 
      + "and (hc2.y = hc1.y+1 or hc2.y = hc1.y or hc2.y = hc1.y-1) " 
      + "and (hc2.z = hc1.z+1 or hc2.z = hc1.z or hc2.z = hc1.z-1) " 
      + "group by hc1.z, hc1.y, hc1.x order by hc1.z, hc1.y, hc1.x").persist()
	neighbourhoodCells.createOrReplaceTempView("neighborhoodCells")
  neighbourhoodCells.show()
  
  spark.udf.register("zScore", (nCellCount: Int, sumHotCells: Int, numCells: Int, x: Int, y: Int, z: Int, mean: Double, standardDeviation: Double) => ((HotcellUtils.calculateZScore(nCellCount, sumHotCells, numCells, x, y, z, mean, standardDeviation))))
  pickupInfo = spark.sql(s"select x, y, z, zScore(spatialWeightSum, sumHotCells, ${numCells}, x, y, z, ${mean}, ${standardDeviation}) as getisOrdStatistic from neighborhoodCells order by getisOrdStatistic desc");
  pickupInfo.createOrReplaceTempView("zScore")
    
  pickupInfo = spark.sql("select x, y, z from zScore")
  pickupInfo.createOrReplaceTempView("finalPickupInfo")
  pickupInfo.show()

  return pickupInfo
}
}