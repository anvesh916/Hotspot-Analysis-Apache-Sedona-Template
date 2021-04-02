package cse512

object HotzoneUtils {

  def ST_Contains(queryRectangle: String, pointString: String ): Boolean = {
    
    // Split the points
    val pt = pointString.split(",")
    val px = pt(0).trim().toDouble
    val py = pt(1).trim().toDouble

    // Split the rectangles
    val rect = queryRectangle.split(",")
    val rx1 = rect(0).trim().toDouble
    val ry1 = rect(1).trim().toDouble
    val rx2 = rect(2).trim().toDouble
    val ry2 = rect(3).trim().toDouble
    
    var (minX, maxX) =  Find_Min_Max(rx1, rx2)
    var (minY, maxY) =  Find_Min_Max(ry1, ry2)
    
    return px >= minX && px <= maxX && py >= minY && py <= maxY
  }

  def Find_Min_Max( a: Double, b: Double): (Double, Double) = {
     if(a < b) {
       return (a, b)
    } else {
       return (b, a)
    } 
  }
  // YOU NEED TO CHANGE THIS PART IF YOU WANT TO ADD ADDITIONAL METHODS

}
