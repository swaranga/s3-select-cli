package com.s3.select.cli;

import java.io.InputStream;
import java.util.Optional;
import java.util.Scanner;
import java.util.concurrent.TimeUnit;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.regions.DefaultAwsRegionProviderChain;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.CSVInput;
import com.amazonaws.services.s3.model.CompressionType;
import com.amazonaws.services.s3.model.ExpressionType;
import com.amazonaws.services.s3.model.InputSerialization;
import com.amazonaws.services.s3.model.JSONInput;
import com.amazonaws.services.s3.model.JSONOutput;
import com.amazonaws.services.s3.model.OutputSerialization;
import com.amazonaws.services.s3.model.ParquetInput;
import com.amazonaws.services.s3.model.SelectObjectContentEvent;
import com.amazonaws.services.s3.model.SelectObjectContentEventVisitor;
import com.amazonaws.services.s3.model.SelectObjectContentRequest;
import com.amazonaws.services.s3.model.SelectObjectContentResult;
import com.amazonaws.util.IOUtils;

public class S3SelectCli {
    private static final String BUCKET_KEY = "bucket";
    private static final String S3_KEY = "key";
    
    public static void main(String[] args) throws Exception {
        CommandLineParser parser = new DefaultParser();
        Options options = getOptions();
        
        CommandLine line = parser.parse(options, args);
        
        if (line.hasOption("help")) {
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("S3SelectCli", options);
            System.exit(1);
        }

        for (String arg : line.getArgList()) {
            System.err.println("Warning: ignoring unknown argument: " + arg);
        }
        
        String bucket = line.getOptionValue(BUCKET_KEY);
        String key = line.getOptionValue(S3_KEY);
        String format = line.getOptionValue("format");
        
        AmazonS3 s3Client = AmazonS3ClientBuilder.standard()
                .withRegion(new DefaultAwsRegionProviderChain().getRegion())
                .withCredentials(new DefaultAWSCredentialsProviderChain())
                .build();

        Scanner scanner = new Scanner(System.in);

        while (true) {
            Optional<String> query = getNextQuery(scanner, bucket, key);

            if (query.isPresent()) {
                execute(s3Client, bucket, key, query.get(), format);
            } else {
                System.out.println("BYE");
                break;
            }
        }

        scanner.close();
    }
    
    private static Options getOptions() {
        Options options = new Options();
        options.addOption(Option.builder("b").longOpt("bucket").hasArg().desc("The S3 bucket").required().build());
        options.addOption(Option.builder("k").longOpt("key").hasArg().desc("The S3 file key").required().build());
        options.addOption(Option.builder("f").longOpt("format").hasArg().desc("The file format").required().build());
        return options;
    }

    private static Optional<String> getNextQuery(Scanner in, String bucket, String key) throws Exception {
        String query = "";
        printPrompt(bucket, key);

        while (true) {
            String line = in.nextLine();

            if ("".equals(query) && "exit".equals(line)) {
                return Optional.empty();
            }
            
            if("".equals(query) && line != null && line.trim().isEmpty()) {
                printPrompt(bucket, key);
                continue;
            }

            if (line != null && !line.trim().isEmpty()) {
                query = query + " " + line;

                if (line.endsWith(";")) {
                    return Optional.of(query);
                }
            }
        }
    }
    
    private static void printPrompt(String bucket, String key) {
        System.out.println(String.format("s3://%s/%s", bucket, key));
        System.out.print("s3_select> ");
    }

    private static void execute(AmazonS3 s3Client, String bucket, String key, String query, String format) {
        try {
            run(s3Client, bucket, key, query, format);
        } catch (Exception ex) {
            System.out.println("ERROR: " + ex.getMessage());
        }
    }

    private static void run(AmazonS3 s3Client, String bucket, String key, String query, String format) throws Exception {
        SelectObjectContentRequest request = generateBaseRequest(bucket, key, query, format);
        final long startTime = System.nanoTime();

        try (SelectObjectContentResult result = s3Client.selectObjectContent(request)) {
            InputStream resultInputStream = result.getPayload()
                    .getRecordsInputStream(new SelectObjectContentEventVisitor() {
                        @Override
                        public void visit(SelectObjectContentEvent.StatsEvent event) {
                            System.out.println("Bytes Scanned: " + event.getDetails().getBytesScanned()
                                    + ", Bytes Processed: " + event.getDetails().getBytesProcessed());
                        }

                        @Override
                        public void visit(SelectObjectContentEvent.EndEvent event) {
                            System.out.println("Query time: "
                                    + TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTime) + " ms");
                        }
                    });

            IOUtils.copy(resultInputStream, System.out);
        }
    }

    private static SelectObjectContentRequest generateBaseRequest(String bucket, String key, String query, String format) {
        SelectObjectContentRequest request = new SelectObjectContentRequest();
        request.setBucketName(bucket);
        request.setKey(key);
        request.setExpression(query);
        request.setExpressionType(ExpressionType.SQL);

        InputSerialization inputSerialization = new InputSerialization();
        
        
        if("parquet".equals(format)) {
            inputSerialization.setParquet(new ParquetInput());
        }
        
        if("csv".equals(format)) {
            inputSerialization.setCsv(new CSVInput());
        }
        
        if("json".equals(format)) {
            inputSerialization.setJson(new JSONInput());
        }
        
        inputSerialization.setCompressionType(CompressionType.NONE);
        request.setInputSerialization(inputSerialization);

        OutputSerialization outputSerialization = new OutputSerialization();
        outputSerialization.setJson(new JSONOutput());
        request.setOutputSerialization(outputSerialization);

        return request;
    }
}
