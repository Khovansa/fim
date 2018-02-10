# fim-cmp

Implements a number of parallel Frequent Itemsets generation algorithms using the Spark framework and allows running them via a command-line interface.

The 'src/main/scripts' folder contains an example script for each supported algorithm.<br/>
The algorithms' performance has been tested in AWS, the parameters are in **'src/main/scripts/aws'** and the results are in **'docs'** folder.

**Command-line interface:**
  * To see the list of supported algorithms, use '_--help_' option. <br/>
   At the moment they are: FP_GROWTH, BIG_FIM, and FIN. <br/>
  * The FP_GROWTH one just uses the standard Spark implementation.
  * To see the list of available options for the chosen algorithm, use '_\<algorithm name> --help_'.

 
 
