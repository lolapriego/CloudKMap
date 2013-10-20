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
  private static String key        = "shared";
  private AmazonS3 s3Client;
  private List<String> paths;
  private String filename; //name of the input file at our bucket

  public Splitter(String filename){
    this.filename = filename;
  }

  //This method is going to provide the a list of url whith the splitted objects
  public static List<String> inputSplitter (){
    s3Client = new AmazonS3Client(new PropertiesCredentials(S3Sample.class.getResourceAsStream("AwsCredentials.properties"))); //TODO: how to introduce it at this file

        try {
            System.out.println("Downloading an object");
            S3Object s3object = s3Client.getObject(new GetObjectRequest(
                bucketName, key + filename));
            System.out.println("Content-Type: "  +
                s3object.getObjectMetadata().getContentType());
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
    InputStream reader = new BufferedInputStream(input);
    File file;
    OutputStream writer;
    int read = -1;
    byte[] bytesToSave;
    int counter_extension;

    // for each chunk of bytes
      file = new File(filename + counter_extension);
      writer = new BufferedOutputStream(new FileOutputStream(file));
      try {
          writer.write(bytesToSave);
      }
      catch (Exception ex) {
          Log.e("ERROR", ex.getMessage());
      }
      finally {
          writer.flush();
          writer.close();
          reader.close();
      }

          try {
              System.out.println("Uploading a new object to S3 from a file\n");
              s3client.putObject(new PutObjectRequest(
                                   bucketName, key + filename + counter_extension, file));
              paths.add(key + filename + counter_extension);

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
        // end for
  }


}
