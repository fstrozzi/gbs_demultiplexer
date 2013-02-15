
import java.util.zip._
import java.io._
import scala.io._

  val barcodesFile = "/home/strozzif/GBS/Barcode-GBS.txt"
  val barcodesSamples = processBarcodes(barcodesFile)
  val barcodes = barcodesSamples.keys.toSet
  val cutSite = "TGCAG"
  val fastqDir = args(0) 
  var total = 0
  var undetermined = 0
  var cutSiteNotFound = 0
  val writers = openWriters(barcodesSamples) 


  try {
    for(file <- new File(fastqDir).listFiles.filter(_.getName.endsWith(".gz"))) {
      val fileName = file.getPath
      println("Processing "+fileName)
      val (t,u,b) = processFastQ(fileName,cutSite,barcodes,writers)
      total += t
      undetermined += u
      cutSiteNotFound += b
    }
  }
  finally {
    closeWriters(writers)
    println("Total processed sequences: "+total.toString)
    println("Sequences with no cut site found: "+cutSiteNotFound.toString+" ("+(cutSiteNotFound/total.toFloat * 100).toString+" %)")
    println("Undetermined sequences: "+undetermined.toString+" ("+(undetermined/total.toFloat * 100).toString+" %)")
  }
  def openWriters(barcodes: Map[String,String]) : Map[String,BufferedOutputStream] = {
    var writers = Map[String,BufferedOutputStream]()
    barcodes.foreach {elem =>
      writers += elem._1 -> new BufferedOutputStream(new
        FileOutputStream(elem._2+".fastq"),100000)
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

  def closeWriters(writers: Map[String,BufferedOutputStream]) : Unit = {
    writers.foreach {elem =>
      elem._2.close()
    }
  }

  def processFastQ(file: String, cutSite: String, barcodes: Set[String], writers: Map[String,BufferedOutputStream]) = { 
    var localTotal = 0
    var localUndetermined = 0
    var localCutSiteNotFound = 0
    val fastq = new BufferedSource(new GZIPInputStream(new BufferedInputStream(new FileInputStream(file),1000000)),1000000)
    fastq.getLines.grouped(4).foreach {seq => 
      total += 1
      val res = processSeq(seq,cutSite,barcodes)
      if (res.size == 2) {
        writers(res(1)).write((seq(0)+" B:"+res(1)+"\n"+res(0)+"\n+\n"+seq(3).substring(res(1).size)+"\n").getBytes("UTF-8")) 
      }
      else {
        localUndetermined += 1
        if (res(0) == "noCut") {localCutSiteNotFound += 1}
      }
    }
    (localTotal,localUndetermined,localCutSiteNotFound)
  }

  def processSeq(seq: Seq[String], cutSite: String, barcodes: Set[String]) : Array[String] = {
    val trimmed = seq(1).split(cutSite,2)
    if (barcodes.contains(trimmed(0))) {
      Array(cutSite + trimmed(1),trimmed(0)) 
    }
    else {
      if (trimmed.size == 1) {
        Array("noCut")
      }
      else {
        Array("noBarcode")
      }
    }
  }

