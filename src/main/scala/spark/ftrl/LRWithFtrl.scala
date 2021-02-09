package spark.ftrl

import java.io.BufferedReader
import java.io.InputStreamReader
import java.io.PrintWriter
import java.io.File

import org.apache.spark.SparkContext
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ArrayBuilder
import scala.collection.mutable.HashMap
import scala.collection.mutable.Set
import scala.util.Random

class LRWithFtrl(
		val initModelPath : String,
		val modelPath : String,
        val filterPath : String,
		val alpha : Float,
		val beta : Float,
		val lambda1 : Float,
		val lambda2 : Float,
		val hasBias : Boolean,
		val reCheck : Boolean,
		val numIters: Int) extends Serializable {
	@transient var wznMapGlobal = new HashMap[Long,Wzn]
    
    private var fSet = Set[Long]()
	private val TAB = ' '
	private val LOG_MIN = -50d
	private val LOG_MAX = 50d
	private val BETA0 = new KVpair(0,1)

	private def loadInitModel(initModelPath: String, fs: FileSystem): Unit = {
		val path = new Path(initModelPath)
		val mlists = fs.listStatus(path)
		for(mf <- mlists){
			val mp = mf.getPath
			val fsin = fs.open(mp)
			val isr = new InputStreamReader(fsin)
			val br = new BufferedReader(isr)
			var line = br.readLine()
			while(line != null){
				val wgtVec = line.trim.split(TAB)
				val feat = wgtVec(0).toLong
				val w = wgtVec(1).toFloat
				val z = wgtVec(2).toDouble
				val n = wgtVec(3).toDouble
				wznMapGlobal(feat) = new Wzn(w,z,n,0)
				line = br.readLine()
			}
			fsin.close()
			isr.close()
			br.close()
        }
	}

	private def parseLine(line: String, hasBias: Boolean): Sample = {
		//pv,clk,features
		val arr = line.split(TAB)
		val label = arr(0).toInt

		val arrBuilder: ArrayBuilder[KVpair] = ArrayBuilder.make()
		if(hasBias){
			arrBuilder += BETA0
		}
		arr.slice(1, arr.length).foreach(v =>{
            val fArr = v.split(":")
			arrBuilder += new KVpair(fArr(0).toLong,fArr(1).toFloat)
		})
		val features = arrBuilder.result()
		new Sample(label,1-label,features)
	}
	
    private def parseLine(line: String, hasBias: Boolean, feaFilter: Set[Long]): Sample = {
		//pv,clk,features
		val arr = line.split(TAB)
		val label = arr(0).toInt

		val arrBuilder: ArrayBuilder[KVpair] = ArrayBuilder.make()
		if(hasBias){
			arrBuilder += BETA0
		}
		arr.slice(1, arr.length).foreach(v =>{
            val fArr = v.split(":")
            val idx = fArr(0).toLong >> 32
            if( !feaFilter.apply(idx) ){
			    arrBuilder += new KVpair(fArr(0).toLong,fArr(1).toFloat)
            }
		})
		val features = arrBuilder.result()
		new Sample(label,1-label,features)
	}

	private def sgn(a: Double): Int = {
		if(a < 0) -1 else if(a > 0) 1 else 0
	}
	
    private def fabs(a: Double): Double = {
		if(a > 0) a else -a
	}

	private def wznSum(a: Wzn, b: Wzn): Wzn = {
		if ((a.cnt > 0) && (b.cnt > 0)) {
			a.w += b.w
			a.z += b.z
			a.n += b.n
			a.cnt = (a.cnt + b.cnt).toShort
			a
		} else if (a.cnt > 0) a else b
	}

	private def averageWzn(wzn: Wzn): Wzn = {
		if(wzn.cnt == 0) return wzn
		wzn.w = wzn.w / wzn.cnt
		wzn.z = wzn.z / wzn.cnt
		wzn.n = wzn.n / wzn.cnt
		wzn.cnt = 0
		if(reCheck && wzn.w != 0){
			val tmp = -((beta + math.sqrt(wzn.n)) / alpha + lambda2) * wzn.w
			if(tmp > 0) wzn.z = tmp + lambda1 else wzn.z = tmp - lambda1
		}
		if(reCheck && math.abs(wzn.z) <= lambda1) wzn.w = 0f
		wzn
	}

	private def updateSample(wznMap: HashMap[Long,Wzn], sample: Sample): Unit = {
		val clk = sample.clk
		val noclk = sample.noclk
		val features = sample.features
		val size = features.length
		var wgtSum = 0d
		for(i <-0 until(size)){
			val arr = features(i)
			val key = arr.key
			val value = arr.value
			val wzn = wznMap.getOrElseUpdate(key,new Wzn(0,0,0,0))
			wgtSum += wzn.w * value
		}
		if(wgtSum < LOG_MIN) wgtSum = LOG_MIN else if(wgtSum > LOG_MAX) wgtSum = LOG_MAX
		val p = 1.0d / (1.0d + math.exp(-wgtSum))
		val tgrad = noclk * p - clk * (1-p)
		for(i <-0 until(size)){
			val arr = features(i)
			val key = arr.key
			val value = arr.value
			val grad = tgrad * value
			val wzn = wznMap.getOrElseUpdate(key, new Wzn(0,0,0,0))
			val sigma = (math.sqrt(wzn.n + grad*grad) - math.sqrt(wzn.n)) / alpha
			wzn.z = (wzn.z + grad - sigma*wzn.w).toDouble
			wzn.n = (wzn.n + grad*grad).toDouble
			if(math.abs(wzn.z) <= lambda1) wzn.w = 0 else{
				val sgnz = sgn(wzn.z)
				wzn.w = ((sgnz*lambda1 - wzn.z) / ((beta + math.sqrt(wzn.n)) / alpha + lambda2)).toFloat
			}
		wzn.cnt = 1
		}
	}

	private def predictSample(wznMap: HashMap[Long,Wzn], sample: Sample): (Double, (Long, Long)) = {
		val clk = sample.clk
		val noclk = sample.noclk
		val features = sample.features
		val size = features.length
		var wgtSum = 0d
		for(i <-0 until(size)){
			val arr = features(i)
			val key = arr.key
			val value = arr.value
			val wzn = wznMap.getOrElse(key,new Wzn(0,0,0,0))
			wgtSum += wzn.w * value
		}
		if(wgtSum < LOG_MIN) wgtSum = LOG_MIN else if(wgtSum > LOG_MAX) wgtSum = LOG_MAX
		val p = 1.0d / (1.0d + math.exp(-wgtSum))
		(p,(noclk,clk))
	}

	def train(sc: SparkContext, tdata: String, partLimit: Int): Unit = {
		val fs = Utils.getFs(tdata)
		if(initModelPath != null && initModelPath.length > 1){
			loadInitModel(initModelPath, fs)
		}
		
        if(filterPath != null && filterPath.length > 1){
			fSet = Utils.getSet(filterPath)
		}

        val pm = new PartitionManager(tdata, sc, partLimit)
        pm.preprocess()
        var finished = false
        var rdd: RDD[String] = null
        while (!finished) {
            val rst = pm.nextRdd()
            finished = rst._1
            rdd = rst._2
            if (rdd != null) trainRdd(rdd, sc)
        }
        sc.parallelize(wznMapGlobal.toSeq).map(v => v._1.toString + TAB + v._2).repartition(100).saveAsTextFile(modelPath)
    }

    private def trainRdd(rdd: RDD[String], sc: SparkContext): Unit = {
		val data = rdd.map(line => {
			parseLine(line, hasBias, fSet)
		})
		data.cache()

		for(i <-0 until(numIters)){
			val wznMapBC = rdd.context.broadcast(wznMapGlobal)
			val currIterRdd = data.mapPartitionsWithIndex((idx, pIter) => {
				var wznMap = wznMapBC.value
				val iter = Random.shuffle(pIter)
				iter.foreach(sample => {
					updateSample(wznMap, sample)
				})
				wznMap.iterator
			})
			.map(a => (a._1, a._2))
			.reduceByKey((x,y) => {
				wznSum(x,y)
			})
			.map(a => (a._1,averageWzn(a._2)))

			wznMapBC.unpersist()
			currIterRdd.collect().foreach(a => wznMapGlobal.put(a._1,a._2))
		}
		data.unpersist()
	}
	
	def evalue(sc: SparkContext, vdata: String, resPath: String): Unit = {
		var x = 0.0
		var y = 0.0
        var psum = 0.0
        var msum = 0.0
		var sqSum = 0.0
		var rdd: RDD[String] = sc.textFile(vdata) 
		val wznMapBC = rdd.context.broadcast(wznMapGlobal)
		val currRdd = rdd.map(line => {
			parseLine(line, hasBias)
		})
        .mapPartitionsWithIndex((idx, pIter) => {
			val wznMap = wznMapBC.value
			pIter.toList.map(sample => {
				predictSample(wznMap, sample)
			}).iterator
		})
		.reduceByKey((x, y) => (x._1+y._1, x._2+y._2))
		.collect.sortWith((a, b) => (a._1 > b._1))
		.foreach(a => {
				val neg = a._2._1
				val pos = a._2._2
				sqSum += 0.5*(y + y + pos)*neg
				y += pos
				x += neg
                psum += a._1
                msum += fabs(a._1 - pos/(neg+pos))*(neg+pos)
		})
		val auc = sqSum/(x*y)
        val oe = y/psum
        val mae = msum/(x+y)
		sc.parallelize(Seq(auc,oe,mae)).coalesce(1).saveAsTextFile(resPath)
	}
}
