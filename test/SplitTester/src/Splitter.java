import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.ClasspathPropertiesFileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;

public class Splitter {
  private static String bucketName = "cloudkmap";
  private static String key        = "shared/shared/";
  private AmazonS3 s3;

  private List<String> paths;
  private List<File> files;
  private String filename; //name of the input file at our bucket

  public final static int sizeBuffer = 1024 * 64;
  public final static int maxChunkKB = 1024;


  public Splitter(String filename){
    this.filename = filename;
    files = new ArrayList<File>();
    paths = new ArrayList<String>();
  }

  //This method provides a list of url whith the splitted objects
  public List<String> inputSplitter (){
    s3 = new AmazonS3Client(new ClasspathPropertiesFileCredentialsProvider()); // it will take the credentials from the .properties file
    Region usWest2 = Region.getRegion(Regions.US_WEST_2);
    s3.setRegion(usWest2);

        try {
            System.out.println("Downloading an object");
            S3Object s3object = s3.getObject(new GetObjectRequest(bucketName, key + filename));
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
    int countChunk = 1;

    try{
        reader = new BufferedInputStream(input);
      do{
          file = new File(filename + "_ext_" + counter_extension);
          
          System.out.println("FILENAME: " + file.getName());
          System.out.println("PATH: " + file.getPath());
          
          fileOutput = new FileOutputStream(file);
          writer = new BufferedOutputStream(fileOutput);

          System.out.println("Start writing the piece number: " + counter_extension);

          array = new byte[sizeBuffer];
          read = reader.read(array);
          
          System.out.println("READS: " + read);

          while (read > 0 && countChunk != maxChunkKB){
            writer.write(array, 0, read);
            System.out.println("WRITE" + countChunk);
            read = reader.read(array);
            countChunk ++;
          }
          counter_extension ++;
          writer.flush();
          files.add(file);
          countChunk = 0;
        } while (read > 0);

      }
      catch (Exception e) {
          e.printStackTrace();
      }
      finally {
    	  try{
          writer.close();
          reader.close();
          fileOutput.close();
    	  }
    	  catch(IOException e){
    		  e.printStackTrace();
    	  }
      }

      uploader();
  }

  private void uploader (){
    for(File f: files){
          try {
              System.out.println("Uploading a new object to S3 from a file\n");
              s3.putObject(new PutObjectRequest(
                                   bucketName, key + f.getName(), f));
              paths.add(key + f.getName());

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