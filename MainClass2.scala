import java.io.{File, FileWriter}
import java.nio.file.{FileSystemException, Files, StandardCopyOption}
import java.text.SimpleDateFormat
import java.util.Calendar

import scala.collection.mutable
import scala.collection.mutable.ListBuffer


object MainClass2 {

  // This method simply returns list of all files in a given directory
  def getListOfFiles(dir: File): List[File] =
  {
    dir.listFiles.filter(_.isFile).toList
  }

  // This method writes records that follow the required criteria to a new file
  def writeFile(filename: String, record: String, dt: String): Unit =
  {
    // Getting data at (day) level of detail
    val subS = record+","+dt.substring(0,8)

    // Checking if the target file exists, and creating it if not
    if(new File(filename).exists())
    {
      // Reading from target file
      val file = io.Source.fromFile(filename)
      val lines = file.getLines().toList
      file.close()
      for (line <- lines)
      {
        // Splitting each line in file by ','
        val lineArr = line.split(",")

        /*
          For each line in file check if the new record already exists in the file,
          and leave the method if so,
          that matches ( same user, same application, same day )
        */
        if((lineArr(0)+","+lineArr(1)+","+lineArr(2)+","+lineArr(3).substring(0,8)).equals(subS))
          return
      }
    }

    /*
      If otherwise the record didn't exist in the target file,
      Append the new record to the target file
    */
    val fw = new FileWriter(filename, true) ;
    fw.write(record + "," + dt + "\n");
    fw.close()
  } // writeFile method closing bracket

  def main(args: Array[String]): Unit = {
    // Check if user has provided all the arguments
    if(args.length == 4) {
      val dataDir = args(0)
      val segmentDataDir = args(1)
      val rulesDataDir = args(2)
      val outputDir = args(3)

      // Checking if rules, segment files and data directory exist
      if(!new File(segmentDataDir).exists() || !new File(rulesDataDir).exists() || !new File(dataDir).exists()) {
        println("Can't find rules or segment files, please provide the full path and try again!")
        System.exit(1)
      }

      println("Listening on directory: " + dataDir)

      /*
      Getting today's date to append it to the target file name,
      because we only need the target customers data of only one day,
      and we don't want that file to be too large because we read from it a lot.
    */
      val format = "yyyy-MM-dd"
      val now = Calendar.getInstance().getTime()
      val dateFormatter = new SimpleDateFormat(format)
      val today = dateFormatter.format(now)

      // Initializing working directories and files
      val workingDir = dataDir
      val segmentFile = segmentDataDir
      val rulesFile = rulesDataDir
      val processedDir = "Processed"
      val targetFile = outputDir
      val directory = new File(workingDir + processedDir)

      // Creating the processed directory if it doesn't exist
      if (!directory.exists)
        directory.mkdir

      // Reading segment file
      val segments = io.Source.fromFile(segmentFile)
      val segmentList = segments.getLines().toList
      segments.close()

      // Reading Rules file
      val rules = io.Source.fromFile(rulesFile)
      var rulesIdsList = new ListBuffer[String]
      var rulesThresholdList = new ListBuffer[ListBuffer[Int]]

      // Populating rules ids list and rules threshold list
      for (rule <- rules.getLines()) {
        val cols = rule.split(",")
        rulesIdsList += cols(0)
        var tmp = new ListBuffer[Int]
        tmp += Integer.parseInt(cols(2))
        tmp += Integer.parseInt(cols(3))
        tmp += Integer.parseInt(cols(4))
        rulesThresholdList += tmp
      }
      rules.close()

      // Creating rules dictionary
      val rulesDict = (rulesIdsList.zip(rulesThresholdList)).toMap


      // Handling stream of files until application shutdown
      while (true) {
        /*
        When trying to read new files it might raise an exception
        that files are already in use by another service
      */
        try {
          // Getting list of all files existing in the working directory
          var files = getListOfFiles(new File(workingDir))

          for (f <- files) {
            // Checking for file name to only work with Data files
            if (f.getName().contains("DATA_")) {
              // Reading each file in the directory
              val dataFile = io.Source.fromFile(workingDir + f.getName)
              for (line <- dataFile.getLines) {
                // For each line in the file, capture the data to filter the records
                val Array(msisdn, appId, traffic, datetime) = line.split(",")
                val hour = datetime.substring(8,10)
                /*
                  Checking if the phone number exists in the segment list
                  and the application id exists in our partners' applications list
                 */
                if (segmentList.contains(msisdn) && rulesIdsList.contains(appId)) {
                  // Checking if the traffic has exceeded the application volume threshold
                  if (Integer.parseInt(hour) >= rulesDict(appId)(0) && Integer.parseInt(hour) <= rulesDict(appId)(1) && Integer.parseInt(traffic) >= rulesDict(appId)(2)) {
                    // Write the new record to the 'daily' target file
                    writeFile(targetFile + "_" + today + ".csv", msisdn + "," + appId + "," + traffic, datetime)
                  }
                }
              }
              /*
                Closing the data file and moving it from it's current directory
                to the new processed directory so that we won't process it again
              */
              dataFile.close
              val d1 = new File(workingDir + f.getName).toPath
              val d2 = new File(directory + "\\" + f.getName).toPath
              Files.move(d1, d2, StandardCopyOption.ATOMIC_MOVE)
            }
          } // For each file in files closing bracket
        } // Try closing bracket
        catch {
          // Handling the exception when new data file streamed in the directory
          case e: FileSystemException => println("Processing new files..")
          case e: Exception => println("Unknown Error has occurred..")
        }

      } // While loop closing bracket
    } // If args provided closing bracket
    else
    {
      println("Please provide the required arguments..")
    }
  } // Main method closing bracket
}
