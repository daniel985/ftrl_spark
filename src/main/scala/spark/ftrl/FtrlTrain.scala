package spark.ftrl

import com.esotericsoftware.kryo.Kryo
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.serializer.KryoRegistrator

import scala.collection.mutable.HashMap

class MyRegistrator extends KryoRegistrator {
	override def registerClasses(kryo: Kryo) {
		kryo.register(classOf[LRWithFtrl])
		kryo.register(classOf[Wzn])
		kryo.register(classOf[HashMap[String, Wzn]])
		kryo.register(classOf[String])
		kryo.register(classOf[Int])
		kryo.register(classOf[Double])
		kryo.register(classOf[Short])
		kryo.register(classOf[Boolean])
	}
}

object FtrlTrain {

	def main(args: Array[String]): Unit = {
		var ftrlConf = ""
		var filterPath = ""
		var trainPath = ""
		var modelPath = ""
		var initModelPath = ""
		var evalPath = ""
		var evalResultPath = ""
        var partLimit = 1

		if(args.length == 7){
			ftrlConf = args(0)
			filterPath = args(1)
			initModelPath = args(2)
			modelPath = args(3)
			trainPath = args(4)
			evalPath = args(5)
			evalResultPath = args(6)
        } else if(args.length == 8){
			ftrlConf = args(0)
			filterPath = args(1)
			initModelPath = args(2)
			modelPath = args(3)
			trainPath = args(4)
			evalPath = args(5)
			evalResultPath = args(6)
            partLimit = args(7).toInt
		} else {
			System.err.println("FtrlTrain: <ftrlConf> <filterPath> <initModelPath> <modelPath> <trainPath> <evalPath> <evalResultPath>")
		}

		val confMap = Utils.parseConf(ftrlConf)
		val alpha = confMap.getOrElse("alpha", "1.0").toFloat
		val beta = confMap.getOrElse("beta", "1.0").toFloat
		val lambda1 = confMap.getOrElse("lambda1", "1.0").toFloat
		val lambda2 = confMap.getOrElse("lambda2", "1.0").toFloat
		val hasBias = confMap.getOrElse("hasBias", "true").toBoolean
		val reCheck = confMap.getOrElse("reCheck", "false").toBoolean
		val numIters = confMap.getOrElse("numIterations", "1").toInt

		val conf = new SparkConf().setAppName("spark_ftrl")
		val sc = new SparkContext(conf)
		val ftrl = new LRWithFtrl(initModelPath, modelPath, filterPath, alpha, beta, lambda1, lambda2, hasBias, reCheck, numIters)
		ftrl.train(sc, trainPath, partLimit)
		//ftrl.train(sc, trainPath)
		if(evalPath.length > 1 && evalResultPath.length > 1) { 
			ftrl.evalue(sc, evalPath, evalResultPath)
		}
	}
}
