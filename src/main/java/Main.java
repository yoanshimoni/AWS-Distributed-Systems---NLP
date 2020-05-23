
import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.ec2.model.InstanceType;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClientBuilder;
import com.amazonaws.services.elasticmapreduce.model.*;
import com.sun.glass.ui.CommonDialogs;
import org.apache.log4j.BasicConfigurator;


public class Main {

    private final static String
            JAR = "s3://maorrockyjars/step_1.jar",
            OUTPUT = "s3://maorrockyjars/output/\n",
            LOGS = "s3://maorrockyjars/logs/\n",
            REGION = "us-east-1",
            KEY_NAME = "maor_dsp202",
            DATA_SET_1GRAM = "s3://datasets.elasticmapreduce/ngrams/books/20090715/eng-us-all/2gram/data\n",
            DATA_1GRAM_a_ONLY = "s3://maorrockyjars/z_short.txt",
            TERMINATE = "TERMINATE_JOB_FLOW";


    public static void main(String[] args) {
        BasicConfigurator.configure();

        System.out.println("Running program");


        AWSCredentialsProvider credentialsProvider = new AWSStaticCredentialsProvider(new ProfileCredentialsProvider().getCredentials());
        AmazonElasticMapReduce mapReduce = AmazonElasticMapReduceClientBuilder
                .standard()
                .withRegion(REGION)
                .withCredentials(credentialsProvider)
                .build();

        System.out.println("created EMR");

        HadoopJarStepConfig stepOneConfig = new HadoopJarStepConfig()
                .withJar(JAR)
                .withMainClass("StepOne")
                .withArgs(DATA_1GRAM_a_ONLY, OUTPUT);
        StepConfig stepOne = new StepConfig()
                .withName("StepOne")
                .withHadoopJarStep(stepOneConfig)
                .withActionOnFailure(TERMINATE);

        System.out.println("create step one");
        JobFlowInstancesConfig instances = new JobFlowInstancesConfig()
                .withInstanceCount(3)
                .withMasterInstanceType(InstanceType.M4Large.toString())
                .withSlaveInstanceType(InstanceType.M4Large.toString())
                .withHadoopVersion("2.7.3")
                .withEc2KeyName(KEY_NAME)
                .withKeepJobFlowAliveWhenNoSteps(false)
                .withPlacement(new PlacementType("us-east-1a"));

        System.out.println("create instances");

        RunJobFlowRequest runFlowRequest = new RunJobFlowRequest()
                .withName("Assignment2")
                .withInstances(instances)
                .withSteps(stepOne)
                .withLogUri(LOGS)
                .withServiceRole("EMR_DefaultRole")
                .withJobFlowRole("EMR_EC2_DefaultRole")
                .withReleaseLabel("emr-5.11.0");

        System.out.println("create runFlowRequest");

        RunJobFlowResult runJobFlowResult = mapReduce.runJobFlow(runFlowRequest);
        String jobFlowId = runJobFlowResult.getJobFlowId();
        System.out.println("cluster id : " + jobFlowId);
    }
}
