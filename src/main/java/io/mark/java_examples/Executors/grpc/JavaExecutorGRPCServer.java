package io.mark.java_examples.Executors.grpc;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;


public class JavaExecutorGRPCServer
{
	final static Logger logger = LoggerFactory.getLogger(JavaExecutorGRPCServer.class);
	Server server;
	public boolean startServer()  {
		try {
			logger.info("Starting JavaExecutorGRPCServer");
			// Let's create uploaded directory if it does not exist
			Path uploadDir = Paths.get(System.getProperty("java.io.tmpdir") + File.separator+"uploaded");
			if (!Files.exists(uploadDir))
				Files.createDirectory(uploadDir);

			 server = ServerBuilder.forPort(8080)
					.addService(new FunctionService())
					.build();

			server.start();
			System.out.println("JavaExecutorGRPCServer is ready");
			logger.info("JavaExecutorGRPCServer is ready");
			//server.awaitTermination();
		}catch (java.io.IOException e){
		    logger.error("Exception occured",e);
			return false;
		}
		return true;

	}

	public static void main( String[] args ) throws Exception
		      {
			      JavaExecutorGRPCServer jes=new JavaExecutorGRPCServer();
				    jes.startServer();
				  jes.server.awaitTermination();
			 }






}


