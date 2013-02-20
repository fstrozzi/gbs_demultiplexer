
import java.util.zip._
import java.io._
import scala.io._
import akka.actor._ 
import com.typesafe.config.ConfigFactory
import org.rogach.scallop._


case object Close

class Conf(arguments: Seq[String]) extends ScallopConf(arguments) {

  val file = opt[String](name="file",short='f',descr="File with FastQ sequences",required=true)
  val barcodes = opt[String](name="barcodes",short='b',descr="File with barcodes and sample IDs (tab separated)",required=true)
  val cutSite = opt[String](name="cutsite",short='c',descr="Enzyme cut-site reminder",required=true)
  val threads = opt[Int](name="threads",short='t',descr="Number of threads to use")

}

object Demultiplex extends App {

  val conf = new Conf(args)
  println(conf.summary)
  startDemultiplex(conf)


  def startDemultiplex(opts: Conf) {

    val barcodesFile: String = opts.barcodes()
    val barcodesSamples = processBarcodes(barcodesFile)
    val barcodes = barcodesSamples.keys.toSet
    val cutSite: String = opts.cutSite()
    val file: String = opts.file()
    val threads: Int = opts.threads()
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
      val (t,u) = processFastQ(file,cutSite,barcodes,writers)
      total += t
      undetermined += u
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
      writers.foreach {elem =>
        elem._2 ! Close
      }
    }

    def processFastQ(file: String, cutSite: String, barcodes: Set[String], writers: Map[String,ActorRef]) = { 
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

    def processSeq(seq: Seq[String], cutSite: String, barcodes: Set[String]) : Array[String] = {
      val trimmed = seq(1).split(cutSite,2)
      if (barcodes.contains(trimmed(0))) {
        Array(cutSite + trimmed(1),trimmed(0)) 
      }
      else {
        Array(cutSite) 
      }
    }

    class Writer extends Actor {
      private var stream : BufferedOutputStream = _

      def write(data: Array[Byte]) : Unit = {
        stream.write(data) 
      }

      def close : Unit  = {
        this.stream.close() 
      }

      def open(fileName: String) : Unit = {
        stream = new BufferedOutputStream(new GZIPOutputStream(new FileOutputStream(fileName),100000)) 
      }

      def receive = {
        case Close => this.close
        case filename: String => this.open(filename)
        case data: Array[Byte] => this.write(data)
      }
    }
}
