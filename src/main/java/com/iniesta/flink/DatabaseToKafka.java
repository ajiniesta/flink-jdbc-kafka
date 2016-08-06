package com.iniesta.flink;

import java.util.Date;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.jdbc.JDBCInputFormat;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;

public class DatabaseToKafka {

	public static void main(String[] args) throws Exception{
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		
		TypeInformation[] types = new TypeInformation[]{TypeInformation.of(Long.class), TypeInformation.of(Integer.class)};
		
		DataSource<Tuple2<Long, Integer>> input = env.createInput(JDBCInputFormat.buildJDBCInputFormat()
				.setDrivername(BootstrapDatabase.DB_DRIVER)
				.setDBUrl(BootstrapDatabase.DB_CONNECTION)
				.setQuery("SELECT ts, record FROM RECORDS")
				.setUsername(BootstrapDatabase.DB_USER)
				.setPassword(BootstrapDatabase.DB_PASSWORD)
				.finish(),  new TupleTypeInfo(types));
		input.map(convertToDate()).print();
	}

	@SuppressWarnings("serial")
	private static MapFunction<Tuple2<Long, Integer>, Tuple2<Date, Integer>> convertToDate() {
		return new MapFunction<Tuple2<Long, Integer>, Tuple2<Date, Integer>>() {
			@Override
			public Tuple2<Date, Integer> map(Tuple2<Long, Integer> value) throws Exception {
				return new Tuple2<Date, Integer>(new Date(value.f0), value.f1);
			}
		};
	}
}
