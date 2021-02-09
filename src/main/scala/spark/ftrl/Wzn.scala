package spark.ftrl

class Wzn(
	var w: Float,
	var z: Double,
	var n: Double,
	var cnt: Short) extends Serializable {

	def this() = this(0,0,0,0)
	override def toString() = "%s %s %s".format(w,z,n)
}
