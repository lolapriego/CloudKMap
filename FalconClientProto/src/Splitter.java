import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.google.protobuf.InvalidProtocolBufferException;
// import our task class
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.File;
import java.io.IOException;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.PutObjectRequest;

public class Splitter {
  private static String bucketName = "cloudkmap";
  private static String key        = "shared/";
  private AmazonS3 s3Client;

  private List<String> paths;
  private List<File> files;
  private String filename; //name of the input file at our bucket

  public final static int sizeBuffer = 1024;
  public final static int maxChunkKB = 64 * 1024;


  public Splitter(String filename){
    this.filename = filename;
    files = new ArrayList<File>();
    paths = new ArrayList<String>;
  }

  //This method provides a list of url whith the splitted objects
  public static List<String> inputSplitter (){
    s3Client = new AmazonS3Client(new ClasspathPropertiesFileCredentialsProvider()); // it will take the credentials from the .properties file
    Region usWest2 = Region.getRegion(Regions.US_WEST_2);
    s3Client.setRegion(usWest2);

        try {
            System.out.println("Downloading an object");
            S3Object s3object = s3Client.getObject(new GetObjectRequest(bucketName, key + filename));
            System.out.println("Content-Type: "  + s3object.getObjectMetadata().getContentType());
            splitter(s3object.getObjectContent());
        } catch (AmazonServiceException ase) {
            System.out.println("Caught an AmazonServiceException, which" +
                " means your request made it " +
                    "to Amazon S3, but was rejected with an error response" +
                    " for some reason.");
            System.out.println("Error Message:    " + ase.getMessage());
            System.out.println("HTTP Status Code: " + ase.getStatusCode());
            System.out.println("AWS Error Code:   " + ase.getErrorCode());
            System.out.println("Error Type:       " + ase.getErrorType());
            System.out.println("Request ID:       " + ase.getRequestId());
        } catch (AmazonClientException ace) {
            System.out.println("Caught an AmazonClientException, which means"+
                " the client encountered " +
                    "an internal error while trying to " +
                    "communicate with S3, " +
                    "such as not being able to access the network.");
            System.out.println("Error Message: " + ace.getMessage());
        }
        finally {
          return paths;
        }
    }


  // Read the file from shared folder. Split it and upload it into shared/filename.
  // Set the list of url of the pieces
  private void splitter(InputStream input){
    BufferedInputStream reader = null;
    File file = null;
    FileOutputStream fileOutput = null;
    BufferedOutputStream writer = null;

    int read = -1;
    byte [] array;
    int counter_extension = 0;
    int countChunkLimit = 1;

    try{
        reader = new BufferedReader(new InputStreamReader(input));
      do{
          file = new File(filename + "_ext_" + counter_extension);
          fileOutput = new FileOutputStream(file);
          writer = new BufferedOutputStream(fileOutput);

          System.out.println("Start writing the piece number: " + counter_extension);

          array = new byte[sizeBuffer];
          read = reader.read(array);

          while (read != -1 && countChunkLimit != maxChunkKB){
            writer.write(array, 0, read);
            read = reader.read(array);
            countChunkLimit ++;
          }
          counter_extension ++;
          writer.flush();
          files.add(file);
        } while (read != -1);

      }
      catch (Exception ex) {
          e.printStackTrace();
      }
      finally {
          writer.close();
          reader.close();
      }

      uploader();
  }

  private void uploader (){
    for(File f: files){
          try {
              System.out.println("Uploading a new object to S3 from a file\n");
              s3client.putObject(new PutObjectRequest(
                                   bucketName, key + file.getName(), file));
              paths.add(key + file.getName());

           } catch (AmazonServiceException ase) {
              System.out.println("Caught an AmazonServiceException, which " +
                  "means your request made it " +
                      "to Amazon S3, but was rejected with an error response" +
                      " for some reason.");
              System.out.println("Error Message:    " + ase.getMessage());
              System.out.println("HTTP Status Code: " + ase.getStatusCode());
              System.out.println("AWS Error Code:   " + ase.getErrorCode());
              System.out.println("Error Type:       " + ase.getErrorType());
              System.out.println("Request ID:       " + ase.getRequestId());
          } catch (AmazonClientException ace) {
              System.out.println("Caught an AmazonClientException, which " +
                  "means the client encountered " +
                      "an internal error while trying to " +
                      "communicate with S3, " +
                      "such as not being able to access the network.");
              System.out.println("Error Message: " + ace.getMessage());
          }
    }
  }


}
