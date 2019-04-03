package com.atomscat.springboot.executor;

import com.atomscat.springboot.config.FlinkProperties;
import com.atomscat.springboot.example.SocketWindowWordCount;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.springframework.boot.ExitCodeGenerator;
import org.springframework.boot.SpringApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.core.task.TaskExecutor;

@Slf4j
public class FlinkExecutor {
    private final ApplicationContext appCtx;
    private final TaskExecutor taskExecutor;
    private final FlinkProperties flinkProperties;
    private final StreamExecutionEnvironment flinkEnv;

    public FlinkExecutor(ApplicationContext appCtx, TaskExecutor taskExecutor,
                         FlinkProperties flinkProperties, StreamExecutionEnvironment flinkEnv) {

        this.appCtx = appCtx;
        this.taskExecutor = taskExecutor;
        this.flinkProperties = flinkProperties;
        this.flinkEnv = flinkEnv;
    }

    @EventListener
    public void handleContextRefresh(ContextRefreshedEvent event) {
        taskExecutor.execute(() -> {    // execute in another thread so we don't hold it up
            try {
                log.info("Running flink job " + flinkProperties.getJobName());
                taskExecutor.execute(this::executeFlinkJob);
                //Thread.sleep(flinkProperties.getTerminationGracePeriodMs());
                //conditionallyExitSpringApp(0);
            } catch (Exception e) {
                log.error("Failed to submit flink job", e);
                //conditionallyExitSpringApp(1);
            }
        });
    }

    private void executeFlinkJob() {
        try {
            flinkEnv.execute(flinkProperties.getJobName());
            // the port to connect to
            int port = 9000;

            // get the execution environment
            final StreamExecutionEnvironment env = flinkEnv.getExecutionEnvironment();

            // get input data by connecting to the socket
            DataStream<String> text = env.socketTextStream("localhost", port, "\n");

            // parse the data, group it, window it, and aggregate the counts
            DataStream<SocketWindowWordCount.WordWithCount> windowCounts = text
                    .flatMap(new FlatMapFunction<String, SocketWindowWordCount.WordWithCount>() {
                        @Override
                        public void flatMap(String value, Collector<SocketWindowWordCount.WordWithCount> out) {
                            for (String word : value.split("\\s")) {
                                out.collect(new SocketWindowWordCount.WordWithCount(word, 1L));
                            }
                        }
                    })
                    .keyBy("word")
                    .timeWindow(Time.seconds(5), Time.seconds(1))
                    .reduce(new ReduceFunction<SocketWindowWordCount.WordWithCount>() {
                        @Override
                        public SocketWindowWordCount.WordWithCount reduce(SocketWindowWordCount.WordWithCount a, SocketWindowWordCount.WordWithCount b) {
                            return new SocketWindowWordCount.WordWithCount(a.word, a.count + b.count);
                        }
                    });

            // print the results with a single thread, rather than in parallel
            windowCounts.print().setParallelism(1);

            env.execute("Socket Window WordCount");
        } catch (Exception e) {
            log.error("Failed to submit flink job", e);
            conditionallyExitSpringApp(1);
        }
    }

    private void conditionallyExitSpringApp(int exitCode) {
        if (flinkProperties.isTerminate()) {
            log.info("Terminating flink spring application with application code " + exitCode);
            System.exit(SpringApplication.exit(appCtx, (ExitCodeGenerator) () -> exitCode));
        }
    }

    // Data type for words with count
    public static class WordWithCount {

        public String word;
        public long count;

        public WordWithCount() {
        }

        public WordWithCount(String word, long count) {
            this.word = word;
            this.count = count;
        }

        @Override
        public String toString() {
            return word + " : " + count;
        }
    }
}
