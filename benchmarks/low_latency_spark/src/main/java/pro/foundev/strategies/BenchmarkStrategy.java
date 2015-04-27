/*
 * Copyright 2015 Foundational Development
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package pro.foundev.strategies;

import com.google.common.base.Stopwatch;
import org.apache.commons.math.stat.descriptive.DescriptiveStatistics;
import pro.foundev.reporting.BenchmarkReport;

import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

public abstract class BenchmarkStrategy {
    private static Logger logger = Logger.getLogger(BenchmarkStrategy.class.toString());
    private final String master;

    public BenchmarkStrategy(String master) {

        this.master = master;
    }

    protected String getMaster(){return master;}
    protected String getMasterUrl(){return "spark://" + master + ":7077";}

    protected abstract String getStrategyName();
    protected abstract void executeCheapComputationOnSinglePartition();
    protected abstract void executeComputationOnRangeOfPartitions();
    protected abstract void executeComputationOnPartitionFoundOn2i();

    public BenchmarkReport run(int runs){
        logger.info("Starting benchmark of " + getStrategyName());
        BenchmarkReport report = new BenchmarkReport(getStrategyName());
        exec(new Runnable() {
            @Override
            public void run() {
                executeComputationOnRangeOfPartitions();

            }
        }, "multiple partition lookup", runs, report);
        exec(new Runnable() {
            @Override
            public void run() {
                executeCheapComputationOnSinglePartition();
            }
        }, "cheap single partition lookup", runs,report);
        exec(new Runnable() {
            @Override
            public void run() {
                executeComputationOnPartitionFoundOn2i();
            }
        }, "2i lookup", runs,report);

        logger.info("Ending benchmark of " + getStrategyName());
        return report;
    }
    private void exec(Runnable runnable, String name, int runs, BenchmarkReport report){
        logger.info("Starting run of " + name);
        DescriptiveStatistics stats = new DescriptiveStatistics();

        Stopwatch timer = new Stopwatch();
        for (int i = 0; i < runs; i++) {
            timer.start();
            runnable.run();
            timer.stop();
            logger.info("Time to execute load run #" + i + " it took " + timer);
            stats.addValue(timer.elapsed(TimeUnit.MILLISECONDS));
            timer.reset();
        }
        logger.info("Finished run of " + name);
        report.addLine(name,
                stats.getMin(),
                stats.getMax(),
                stats.getPercentile(50),
                stats.getPercentile(90),
                stats.getMean());
    }
}
