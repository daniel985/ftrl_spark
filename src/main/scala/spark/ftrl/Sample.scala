package spark.ftrl

class Sample(
	val clk: Int,
	val noclk: Int,
	val features: Array[KVpair]) extends Serializable{
}

class KVpair(
	val key: Long,
    val value: Float) extends Serializable{
}
