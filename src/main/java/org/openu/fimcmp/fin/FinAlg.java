package org.openu.fimcmp.fin;

import org.apache.commons.lang3.time.StopWatch;
import org.apache.spark.HashPartitioner;
import org.apache.spark.Partitioner;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.openu.fimcmp.FreqItemset;
import org.openu.fimcmp.algbase.AlgBase;
import org.openu.fimcmp.algbase.F1Context;
import org.openu.fimcmp.result.BitsetFiResultHolderFactory;
import org.openu.fimcmp.result.CountingOnlyFiResultHolderFactory;
import org.openu.fimcmp.result.FiResultHolder;
import org.openu.fimcmp.result.FiResultHolderFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * Implement the main steps of the FIN+ algorithm.
 */
public class FinAlg extends AlgBase<FinAlgProperties> {

    @SuppressWarnings("WeakerAccess")
    public FinAlg(FinAlgProperties props) {
        super(props);
    }

    public static Class[] getClassesToRegister() {
        return new Class[]{
                FinAlgProperties.class, ProcessedNodeset.class, DiffNodeset.class, PpcNode.class,
                FinAlgHelper.class,
                Predicate.class
        };
    }

    public static void main(String[] args) throws Exception {
        FinAlgProperties props = new FinAlgProperties(0.7);
        props.inputNumParts = 1;
        props.isPersistInput = true;
        props.requiredItemsetLenForSeqProcessing = 1;
        props.runType = FinAlgProperties.RunType.PAR_SPARK;
//        props.runType = FinAlgProperties.RunType.SEQ_SPARK;

//        String inputFile = "C:\\Users\\Alexander\\Desktop\\Data Mining\\DataSets\\" + "my.small.txt";
        String inputFile = "C:\\Users\\Alexander\\Desktop\\Data Mining\\DataSets\\" + "pumsb.dat";
        runAlg(props, inputFile);
    }

    private static void runAlg(FinAlgProperties props, String inputFile) throws Exception {
        FinContext ctx = prepare(props, inputFile);

        FiResultHolder resultHolder;
        switch (props.runType) {
            case SEQ_PURE_JAVA:
                resultHolder = ctx.alg.collectResultsSequentiallyPureJava(
                        ctx.resultHolderFactory, ctx.rankTrsRdd, ctx.f1Context,
                        props.requiredItemsetLenForSeqProcessing);
                break;
            case SEQ_SPARK:
                resultHolder = ctx.alg.collectResultsSequentiallyWithSpark(
                        ctx.resultHolderFactory, ctx.rankTrsRdd, ctx.f1Context,
                        props.requiredItemsetLenForSeqProcessing, ctx.sc);
                break;
            case PAR_SPARK:
                resultHolder = ctx.alg.collectResultsInParallel(
                        ctx.resultHolderFactory, ctx.rankTrsRdd, ctx.f1Context, props);
                break;
            default:
                throw new IllegalArgumentException("Unsupported run type " + props.runType);
        }

        outputResults(resultHolder, ctx.f1Context, ctx.sw);
    }

    private static FinContext prepare(FinAlgProperties props, String inputFile) throws Exception {
        FinContext res = new FinContext();
        res.alg = new FinAlg(props);

        res.sw = new StopWatch();
        res.sw.start();

        res.sc = createSparkContext(false, "local", res.sw);

        JavaRDD<String[]> trs = res.alg.readInput(res.sc, inputFile, res.sw);

        res.f1Context = res.alg.computeF1Context(trs, res.sw);
        res.f1Context.printRankToItem();
        res.rankTrsRdd = res.f1Context.computeRddRanks1(trs);

//        res.resultHolderFactory = new BitsetFiResultHolderFactory(res.f1Context.totalFreqItems, 20_000);
        res.resultHolderFactory = new CountingOnlyFiResultHolderFactory(res.f1Context.totalFreqItems);
        return res;
    }

    private FiResultHolder collectResultsInParallel(
            FiResultHolderFactory resultHolderFactory, JavaRDD<int[]> data,
            F1Context f1Context, FinAlgProperties props) {
        int numParts = data.getNumPartitions();
        Partitioner partitioner = new HashPartitioner(numParts);
        JavaPairRDD<Integer, int[]> partToRankTrsRdd =
                data.flatMapToPair(tr -> FinAlgHelper.genCondTransactions(tr, partitioner));
        return FinAlgHelper.findAllFisByParallelFin(
                partToRankTrsRdd, partitioner, resultHolderFactory, f1Context, props);
    }


    private FiResultHolder collectResultsSequentiallyWithSpark(
            FiResultHolderFactory resultHolderFactory, JavaRDD<int[]> rankTrsRdd, F1Context f1Context,
            int requiredItemsetLenForSeqProcessing, JavaSparkContext sc) {

        FiResultHolder rootsResultHolder = resultHolderFactory.newResultHolder();
        List<ProcessedNodeset> rootNodesets = FinAlgHelper.createAscFreqSortedRoots(
                rootsResultHolder, rankTrsRdd, f1Context, requiredItemsetLenForSeqProcessing);
        Collections.reverse(rootNodesets); //nodes sorted in descending frequency, to start the most frequent ones first
        JavaRDD<ProcessedNodeset> rootsRdd = sc.parallelize(rootNodesets);

        //process each subtree
        FiResultHolder initResultHolder = resultHolderFactory.newResultHolder();
        final long minSuppCnt = f1Context.minSuppCnt;
        FiResultHolder subtreeResultHolder = rootsRdd
                .map(pn -> pn.processSubtree(resultHolderFactory, minSuppCnt))
                .fold(initResultHolder, FiResultHolder::uniteWith);

        return rootsResultHolder.uniteWith(subtreeResultHolder);
    }

    private FiResultHolder collectResultsSequentiallyPureJava(
            FiResultHolderFactory resultHolderFactory, JavaRDD<int[]> rankTrsRdd, F1Context f1Context,
            int requiredItemsetLenForSeqProcessing) {

        FiResultHolder resultHolder = resultHolderFactory.newResultHolder();
        List<ProcessedNodeset> rootNodesets = FinAlgHelper.createAscFreqSortedRoots(
                resultHolder, rankTrsRdd, f1Context, requiredItemsetLenForSeqProcessing);
//        Collections.reverse(rootNodesets);

        //process each subtree
        for (ProcessedNodeset rootNodeset : rootNodesets) {
            System.out.println(String.format("Processing subtree of %s", Arrays.toString(rootNodeset.getItemset())));
            long sizeBefore = resultHolder.size();
            rootNodeset.processSubtree(resultHolder, f1Context.minSuppCnt);
            System.out.println(String.format("Done processing subtree of %s: +%s -> %s",
                    Arrays.toString(rootNodeset.getItemset()), resultHolder.size() - sizeBefore, resultHolder.size()));
        }

        return resultHolder;
    }

    private static void outputResults(FiResultHolder resultHolder, F1Context f1Context, StopWatch sw) {
        pp(sw, "Total results: " + resultHolder.size());
//        List<FreqItemset> allFrequentItemsets = resultHolder.getAllFrequentItemsets(f1Context.rankToItem);
//        allFrequentItemsets = allFrequentItemsets.stream().
//                sorted(FreqItemset::compareForNiceOutput2).collect(Collectors.toList());
//        allFrequentItemsets.forEach(System.out::println);
//        pp(sw, "Total results: " + allFrequentItemsets.size());
    }

    private static class FinContext {
        StopWatch sw;
        JavaSparkContext sc;
        F1Context f1Context;
        FinAlg alg;
        JavaRDD<int[]> rankTrsRdd;
        FiResultHolderFactory resultHolderFactory;
    }
}
