
import java.util.zip._
import java.io._
import scala.io._
import akka.actor._ 
import com.typesafe.config.ConfigFactory
import org.rogach.scallop._
import scala.concurrent.Await
import akka.pattern.ask
import akka.util.Timeout
import scala.concurrent.duration._

case object Close

class Conf(arguments: Seq[String]) extends ScallopConf(arguments) {
  val version = "GBS Demultiplex 1.0 Copyright(c) 2013 Francesco Strozzi"
  val input = opt[String](name="input",short='i',descr="File or directory with FastQ sequences (gzipped)",required=true)
  val barcodes = opt[String](name="barcodes",short='b',descr="File with barcodes and sample IDs (tab separated)",required=true)
  val cutSite = opt[List[String]](name="cutsite",short='c',descr="Enzyme cut-site reminder",required=true)
  val threads = opt[Int](name="threads",short='t',default=Some(1),descr="Number of threads to use")
}

object Demultiplex extends App {

  val conf = new Conf(args)
  println(conf.summary)
  startDemultiplex(conf)


  def startDemultiplex(opts: Conf) {
    val barcodesFile: String = opts.barcodes()
    val barcodesSamples = processBarcodes(barcodesFile)
    val barcodes = barcodesSamples.keys.toSet
    val cutSite: List[String] = opts.cutSite()
    val threads: Int = opts.threads()
    val input: String = opts.input()
    var total = 0
    var undetermined = 0

    val customConf = ConfigFactory.parseString("""
      akka.actor.default-dispatcher.fork-join-executor {
           parallelism-max = """+threads.toString+"""
          } 
      """)

    val system = ActorSystem("GBS",ConfigFactory.load(customConf))
    val writers = openWriters(barcodesSamples,system)

    try {
      if (new File(input).isDirectory()) {
        for(file <- new File(input).listFiles.filter(_.getName.endsWith(".gz"))) {
          val fileName = file.getPath
          println("Processing "+fileName)
          val (t,u) = processFastQ(fileName,cutSite,barcodes,writers)
          total += t
          undetermined += u
        }
      }
      else {
        val (t,u) = processFastQ(input,cutSite,barcodes,writers)
        total += t 
        undetermined += u
      }
    }
    finally {
      closeWriters(writers)
      println("Total processed sequences: "+total.toString)
      println("Undetermined sequences: "+undetermined.toString+" ("+(undetermined/total.toFloat * 100).toString+" %)")
      system.shutdown()
    }
   }
    def openWriters(barcodes: Map[String,String], system: ActorSystem) : Map[String,ActorRef] = {
      var writers = Map[String,ActorRef]()
      barcodes.foreach {elem =>
        val w = system.actorOf(Props(new Writer))
        w ! elem._2+".fastq.gz"

        writers += elem._1 -> w 
      }
      writers
    }
      
    def processBarcodes(file: String) : Map[String,String] = {
      var barcodes = Map[String,String]()
      Source.fromFile(file).getLines.foreach {line =>
          if (line(0) != '#') { 
            val elem = line.split("\t")
            barcodes += elem(1) -> elem(0)
          } 
      } 
      barcodes 
    }

    def closeWriters(writers: Map[String,ActorRef]) : Unit = {
      implicit val timeout = Timeout(5 seconds)
      writers.foreach {elem =>
        val future = elem._2 ? Close
        val result = Await.result(future, timeout.duration).asInstanceOf[String]
        println(result)
      }
    }

    def processFastQ(file: String, cutSite: List[String], barcodes: Set[String], writers: Map[String,ActorRef]) = { 
      var total = 0
      var undetermined = 0
      val fastq = new BufferedSource(new GZIPInputStream(new BufferedInputStream(new FileInputStream(file),1000000)),1000000)
      fastq.getLines.grouped(4).foreach {seq => 
        total += 1
        val res = processSeq(seq,cutSite,barcodes)
        if (res.size == 2) {
          writers(res(1)) ! (seq(0)+" B:"+res(1)+"\n"+res(0)+"\n+\n"+seq(3).substring(res(1).size)+"\n").getBytes("UTF-8")
        }
        else {
          undetermined += 1
        }
      }
      (total,undetermined)
    }

    def processSeq(seq: Seq[String], cutSite: List[String], barcodes: Set[String]) : Array[String] = {
      cutSite.foreach {site => 
        val trimmed = seq(1).split(site,2)
        if (barcodes.contains(trimmed(0))) {
          return Array(site + trimmed(1),trimmed(0)) 
        }
      }
        return Array("notFound") 
    }

    class Writer extends Actor {
      private var stream : BufferedOutputStream = _
      private var file : String = _

      def write(data: Array[Byte]) : Unit = {
        stream.write(data) 
      }

      def close : Unit  = {
        this.stream.flush()
        this.stream.close()
        sender ! "File "+this.file+" closed"
      }

      def open(fileName: String) : Unit = {
        stream = new BufferedOutputStream(new GZIPOutputStream(new FileOutputStream(fileName),100000))
        file = fileName
      }

      def receive = {
        case Close => this.close
        case filename: String => this.open(filename)
        case data: Array[Byte] => this.write(data)
      }
    }
}
