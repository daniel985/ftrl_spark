package spark.ftrl

import org.apache.spark.SparkContext
import org.apache.hadoop.fs.Path
import org.apache.spark.rdd.RDD

import scala.collection.immutable.Queue

class PartitionManager(data: String, sc: SparkContext, partLimit: Int) {
	var partitions = 0
	var sizeLimit = 0L
	var totalSize = 0L
	var unLoadedFiles = Queue[(String, Long)]()
	var minExectors = 1

	def preprocess() = {
		val fs = Utils.getFs(data)
		val files = fs.globStatus(new Path(data, "*"))
		files.foreach(status => {
			if (!status.isDirectory() && (status.getLen > 0)) {
				unLoadedFiles = unLoadedFiles.enqueue((status.getPath.toString, status.getLen))
				totalSize += status.getLen
			}
		})
		println("total files : " + unLoadedFiles.length)
		
        val conf = sc.getConf
		try{
			val confExecutors = conf.getInt("minexecutors", 0)
			if(confExecutors > minExectors) minExectors = confExecutors
		} catch {
			case e: Exception => {
				println("get excetion for conf.getInt: minexecutors, exception is: " + e.getMessage)
			}
		}
    }

    private def calPartitions() = {
        val conf = sc.getConf
		val cores = conf.getInt("spark.executor.cores", 1)
		val mems = sc.getExecutorMemoryStatus
		val executors = if(mems.size > minExectors) mems.size else minExectors
		val cacheMem = mems.head._2._1
		println("the mem is :" + cacheMem)
		partitions = cores * executors
		sizeLimit = (executors * cacheMem * 0.5).toLong
		println("the sizeLimit is :" + sizeLimit)
		if(sizeLimit <= 0){
			throw new RuntimeException("sizeLimit = " + sizeLimit)
		}
	}

	def nextRdd(notMerge: Boolean = false): (Boolean, RDD[String]) = {
        calPartitions()
		if ((unLoadedFiles.length == 0)){
			println("there is no left data to be loaded")
			return (true, null)
		}

		var curSize = 0L
        var curCnt = 0
		var files = Queue[String]()
		while(curCnt < partLimit && curSize < sizeLimit && unLoadedFiles.length > 0){
			val (file, q) = unLoadedFiles.dequeue
			unLoadedFiles = q
			files = files.enqueue(file._1)
			curSize += file._2
            curCnt += 1
		}

		val newUrl = Utils.genUrl(files)
		if ((newUrl.length == 0) || (curSize == 0)){
			return (true, null)
		}

		var p = partitions
		if ((curSize < (sizeLimit / 2)) && (notMerge == false)){
			//根据当前取出的文件大小计算需要使用的分区数量
			p = (p / (sizeLimit / curSize)).toInt + 1  //consider the last few files, the data sizes are much less than limit, the partition count should be reduced
		}
		var rdd = sc.textFile(newUrl, p)

		if ((rdd.partitions.size > p)  && (notMerge == false)){
			rdd = rdd.coalesce(p)
		}
		//var rdd = sc.textFile(newUrl)
		var lastRdd = false
		if (unLoadedFiles.length == 0){
			lastRdd = true
		}
		(lastRdd, rdd)
	}
}
