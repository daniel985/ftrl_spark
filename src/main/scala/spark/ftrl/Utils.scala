package spark.ftrl

import java.io.{BufferedReader, FileReader}
import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem

import scala.collection.immutable.Queue
import scala.collection.mutable.HashMap
import scala.collection.mutable.Set

object Utils {
	private val SYMBOL_EQUAL = '='

	def parseConf(conf: String): HashMap[String, String] = {
		val confMap = HashMap[String, String]()
		val inputStream = new BufferedReader(new FileReader(conf))
		var line = inputStream.readLine()
		while((line != null)){
			val index = line.indexOf(SYMBOL_EQUAL)
			require(index > 0, s"Command line argument error: $index, can't find = in augument" + line)
			val key = line.substring(0, index).trim
			val value = line.substring(index+1, line.length).trim
			confMap.put(key, value)
			line = inputStream.readLine()
		}
		inputStream.close()
		confMap
	}
	
    def getSet(path: String): Set[Long] = {
		val pSet = Set[Long]()
		val inputStream = new BufferedReader(new FileReader(path))
		var line = inputStream.readLine()
		while((line != null)){
			pSet.add(line.trim.toLong)
			line = inputStream.readLine()
		}
		inputStream.close()
		pSet
	}

	def getFs(path: String): FileSystem = {
		var slashLeft = 2
		var rootEnd = -2
		var index = 0
		path.foreach(c => {
			if((c == '/' || c == '\\') && (slashLeft > 0)){
			    if(rootEnd != index - 1){
			        rootEnd = index
			        slashLeft -= 1
			    }
		    }
		    index += 1
		})
		val root = path.substring(0, rootEnd + 1)
		val fs = FileSystem.get(new URI(root), new Configuration())
		fs
	}

	def genUrl(files: Queue[String]): String =  {
		if(files.length == 1) return files.head

		var url = ""
		files.foreach(path =>{
			url += path + ","
		})
		url = url.substring(0, url.length - 1) //remove the last ","
		url
	}
}
