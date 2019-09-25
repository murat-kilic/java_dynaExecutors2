package io.mark.java_examples.Executors.grpc;

import io.grpc.Server;
import io.grpc.ServerBuilder;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;


public class JavaExecutorGRPCServer
{
	private void startServer() throws Exception {

		// Let's create uploaded directory if it does not exist
		Path uploadDir=Paths.get(System.getProperty("java.io.tmpdir")+"uploaded");
		if (!Files.exists(uploadDir))
			Files.createDirectory(uploadDir);

		  Server server = ServerBuilder.forPort(8080)
			               .addService(new FunctionService())
			              .build();

		    server.start();
		     System.out.println("Server started");
		          server.awaitTermination();

	}

	public static void main( String[] args ) throws Exception
		      {
			      JavaExecutorGRPCServer jes=new JavaExecutorGRPCServer();
				    jes.startServer();
			 }






}


