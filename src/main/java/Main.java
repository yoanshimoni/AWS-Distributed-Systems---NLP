
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.ec2.model.InstanceType;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClientBuilder;
import com.amazonaws.services.elasticmapreduce.model.*;
import org.apache.log4j.BasicConfigurator;


public class Main {

    private final static String
            JAR_step1 = "s3://maorrockyjars/step_1.jar",
            JAR_step2 = "s3://maorrockyjars/step_2.jar",
            JAR_step3 = "s3://maorrockyjars/step_3.jar",
            OUTPUT = "s3://maorrockyjars/output_ngram_abc_only/",
            LOGS = "s3://maorrockyjars/logs_ngram_abc_only/",
            REGION = "us-east-1",
            KEY_NAME = "maor_dsp202",
//            DATA_1GRAM = "s3://datasets.elasticmapreduce/ngrams/books/20090715/eng-us-all/1gram/data",
            DATA_1GRAM = "s3://maorrockyjars/googlebooks-eng-all-1gram-20120701-z",
//            DATA_2GRAM = "s3://datasets.elasticmapreduce/ngrams/books/20090715/eng-us-all/2gram/data",
            DATA_2GRAM = "s3://maorrockyjars/googlebooks-eng-all-2gram-20120701-zy",
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
                .withJar(JAR_step1)
                .withMainClass("StepOne")
                .withArgs(DATA_1GRAM, DATA_2GRAM, OUTPUT+ "outputStepOne/");
        StepConfig stepOne = new StepConfig()
                .withName("StepOne")
                .withHadoopJarStep(stepOneConfig)
                .withActionOnFailure(TERMINATE);

        System.out.println("created step one");

        HadoopJarStepConfig stepTwoConfig = new HadoopJarStepConfig()
                .withJar(JAR_step2)
                .withMainClass("StepTwo")
                .withArgs(OUTPUT+ "outputStepOne/", OUTPUT + "outputStepTwo/");

        StepConfig stepTwo = new StepConfig()
                .withName("StepTwo")
                .withHadoopJarStep(stepTwoConfig)
                .withActionOnFailure(TERMINATE);

        System.out.println("created step two");

        HadoopJarStepConfig stepThreeConfig = new HadoopJarStepConfig()
                .withJar(JAR_step3)
                .withMainClass("StepThree")
                .withArgs(OUTPUT + "outputStepTwo/", OUTPUT + "outputStepThree/");

        StepConfig stepThree = new StepConfig()
                .withName("StepThree")
                .withHadoopJarStep(stepThreeConfig)
                .withActionOnFailure(TERMINATE);

        System.out.println("created step three");

        JobFlowInstancesConfig instances = new JobFlowInstancesConfig()
                .withInstanceCount(10)
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
                .withSteps(stepOne,stepTwo,stepThree)
//                .withSteps(stepThree)
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
