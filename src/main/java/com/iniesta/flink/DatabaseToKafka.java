package com.iniesta.flink;

import java.io.IOException;
import java.util.Properties;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.jdbc.JDBCInputFormat;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class DatabaseToKafka {

	@SuppressWarnings({ "rawtypes", "unchecked" })
	public static void main(String[] args) throws Exception {
		ParameterTool parameters = ParameterTool.fromArgs(args);
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		TypeInformation[] types = new TypeInformation[] { TypeInformation.of(Long.class),
				TypeInformation.of(Integer.class) };

		DataSource<Tuple> input = env.createInput(JDBCInputFormat.buildJDBCInputFormat() //
				.setDrivername(BootstrapDatabase.DB_DRIVER) //
				.setDBUrl(BootstrapDatabase.DB_CONNECTION) //
				.setQuery("SELECT ts, record FROM RECORDS") //
				.setUsername(BootstrapDatabase.DB_USER) //
				.setPassword(BootstrapDatabase.DB_PASSWORD) //
				.finish(), new TupleTypeInfo(types));
		input.map(convertToFlatStrings()).returns(String.class).output(kafkaOutput())
				.withParameters(parameters.getConfiguration());
		env.execute();
		// input.map(flat()).output(kafkaOutput());
	}

	@SuppressWarnings("serial")
	private static MapFunction<Tuple, String> convertToFlatStrings() {
		return new MapFunction<Tuple, String>() {
			@Override
			public String map(Tuple value) throws Exception {
				String ret = "";
				for (int i = 0; i < value.getArity(); i++) {
					ret += value.getField(i) + ",";
				}
				ret = ret.substring(0, ret.length() - 1);
				return ret;
			}
		};
	}

	@SuppressWarnings({ "serial", "unused" })
	private static OutputFormat<String> mockOutput() {
		return new OutputFormat<String>() {

			@Override
			public void configure(Configuration parameters) {
				System.out.println("Configuring...");
			}

			@Override
			public void open(int taskNumber, int numTasks) throws IOException {
				System.out.println("Opening..." + taskNumber + "/" + numTasks);
			}

			@Override
			public void writeRecord(String record) throws IOException {
				System.out.println("Writing... " + record);
			}

			@Override
			public void close() throws IOException {
				System.out.println("Close");
			}
		};
	}

	@SuppressWarnings("serial")
	private static OutputFormat<String> kafkaOutput() {
		return new OutputFormat<String>() {

			private Producer<String, String> producer;

			private String topic;

			private boolean mock;

			private Properties props;

			@Override
			public void configure(Configuration parameters) {
				topic = parameters.getString("kafka.topic", "sample-topic");
				mock = parameters.getBoolean("kafka.mock", true);
				props = new Properties();
				props.put("bootstrap.servers", parameters.getString("bootstrap.servers", "localhost:9092"));
				props.put("acks", parameters.getString("acks", "all"));
				props.put("retries", parameters.getInteger("retries", 0));
				props.put("batch.size", parameters.getInteger("batch.size", 16384));
				props.put("linger.ms", parameters.getInteger("linger.ms", 1));
				props.put("buffer.memory", parameters.getInteger("buffer.memory", 33554432));
				props.put("key.serializer", parameters.getString("key.serializer",
						"org.apache.kafka.common.serialization.StringSerializer"));
				props.put("value.serializer", parameters.getString("value.serializer",
						"org.apache.kafka.common.serialization.StringSerializer"));
			}

			@Override
			public void open(int taskNumber, int numTasks) throws IOException {
				if (mock) {
					producer = new MockProducer<>(true, new StringSerializer(), new StringSerializer());
				} else {
					producer = new KafkaProducer<>(props);
				}
			}

			@Override
			public void writeRecord(String record) throws IOException {
				producer.send(new ProducerRecord<String, String>(topic, record));
			}

			@Override
			public void close() throws IOException {
				producer.close();
			}
		};
	}

}
