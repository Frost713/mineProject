package fsImage.util;

import com.google.common.base.Preconditions;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.function.*;
import java.util.stream.Collector;

/**
 * @author w9006271
 * @since 2020/12/22
 */
public class BatchCollector<T> implements Collector<T, List<T>, List<T>> {

    private final int batchSize;
    private final Consumer<List<T>> batchProcessor;

    public BatchCollector(int batchSize, Consumer<List<T>> batchProcessor) {
        Preconditions.checkNotNull(batchProcessor);
        Preconditions.checkArgument(batchSize > 0, "batchSize必须大于0");
        this.batchSize = batchSize;
        this.batchProcessor = batchProcessor;
    }

    @Override
    public Supplier<List<T>> supplier() {
        return ArrayList::new;
    }

    @Override
    public BiConsumer<List<T>, T> accumulator() {
        return (ts, t) -> {
            ts.add(t);
            if (ts.size() >= batchSize) {
                batchProcessor.accept(ts);
                ts.clear();
            }
        };
    }

    @Override
    public BinaryOperator<List<T>> combiner() {
        return (ts, ots) -> {
            if (ts.size() > 0) {
                batchProcessor.accept(ts);
            }
            if (ots.size() > 0) {
                batchProcessor.accept(ots);
            }
            return Collections.emptyList();
        };
    }

    @Override
    public Function<List<T>, List<T>> finisher() {
        return ts -> {
            if (ts.size() > 0) {
                batchProcessor.accept(ts);
            }
            return Collections.emptyList();
        };
    }

    @Override
    public Set<Characteristics> characteristics() {
        return Collections.emptySet();
    }
}