import com.amazonaws.SdkClientException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.PutObjectRequest;

import java.io.File;
import java.io.IOException;

public class UploadJar {
    public static void main(String[] args) throws IOException {
        uploadJar("step_1.jar", "/home/maor/Desktop/DSP202/ass2_202/out/artifacts/step_1_jar/step_1.jar");

    }

    public static void uploadJar(String name, String pathTo) {
        String REGION = "us-east-1";
        String bucketName = "maorrockyjars";
        String stringObjKeyName = name;
        String fileObjKeyName = name;
        String fileName = pathTo;
        System.out.println("Started uploading " + fileObjKeyName);
        try {
            //This code expects that you have AWS credentials set up per:
            // https://docs.aws.amazon.com/sdk-for-java/v1/developer-guide/setup-credentials.html
            AmazonS3 s3Client = AmazonS3ClientBuilder.standard()
                    .withRegion(REGION)
                    .build();

            // Upload a text string as a new object.
            s3Client.putObject(bucketName, stringObjKeyName, "Uploaded String Object");

            // Upload a file as a new object with ContentType and title specified.
            PutObjectRequest request = new PutObjectRequest(bucketName, fileObjKeyName, new File(fileName));

            s3Client.putObject(request);
        } catch (SdkClientException e) {
            // The call was transmitted successfully, but Amazon S3 couldn't process
            // it, so it returned an error response.
            e.printStackTrace();
        }// Amazon S3 couldn't be contacted for a response, or the client
// couldn't parse the response from Amazon S3.
        System.out.println("finished uploading");
    }


}
