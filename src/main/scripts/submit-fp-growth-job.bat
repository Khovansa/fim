%SPARK_HOME%\bin\spark-submit --class org.openu.fimcmp.cmdline.CmdLineRunner --master spark://192.168.1.68:7077 --driver-memory 1200m --driver-cores 2 --executor-memory 1200m --total-executor-cores 1 --executor-cores 1 --num-executors 2 file://c/projects/fim-cmp/target/fim-cmp-1.0-SNAPSHOT.jar FP_GROWTH     --spark-master-url spark://192.168.1.68:7077 --input-file-name pumsb.dat --min-supp 0.8 --input-parts-num 3 --persist-input false --cnt-only true --print-intermediate-res false --print-all-fis false
