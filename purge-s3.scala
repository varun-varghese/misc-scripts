// to celete previous

import java.time.Instant
import java.time.ZoneId
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter

import org.apache.hadoop.fs.{FileSystem, FileStatus, LocatedFileStatus, Path, PathFilter}
import java.net.URI
import java.io.FileNotFoundException

def getUrl(base: String, date: String): String = {
    return "%s%s/".format(base, date)
}

val datePartition: String = DateTimeFormatter.ofPattern("yyyyMMdd").format(
    ZonedDateTime.ofInstant(Instant.now(), ZoneId.of("UTC")).minusDays(1))
    
val bucketUrl: String = getUrl("s3://<bucket>/<path>/dateid=",datePartition)

val hadoopFs: FileSystem = FileSystem.get(new URI(src), sc.hadoopConfiguration)

val srcPath = new Path(bucketUrl)

hadoopFs.listStatus(srcPath)
            .filter(_.isFile)
            .foreach(file => {
                println(file)
                try {
                    if(hadoopFs.delete(file.getPath(), true) == true) {
                        println("Successfully deleted " + file)
                    }
                } catch {
                    case f: FileNotFoundException => println("Exception when deleting " + file + " since it does not exists!!!")
                    case _: Throwable => println("Exception when deleting " + file + ", Got some other kind of exception!!!")
                }
            })

try {
        if(hadoopFs.delete(srcPath, true) == true) {
            println("Successfully deleted " + srcPath)
        }
} catch {
    case f: FileNotFoundException => println("Exception when deleting " + srcPath + " since it does not exists!!!")
    case _: Throwable => println("Exception when deleting " + srcPath + ", Got some other kind of exception!!!")
}
