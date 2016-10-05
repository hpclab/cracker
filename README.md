Cracker
=======

Crumbling large graphs into connected components

Abstract—Finding connected components is a fundamental task in applications dealing with graph analytics, such as social network
analysis, web graph mining and image processing. The exponentially growing size of today’s graphs has required the definition of new
computational models and algorithms for their efficient processing on highly distributed architectures. In this paper we present
CRACKER, an efficient iterative MapReduce-like algorithm to detect connected components in large graphs. The strategy of CRACKER
is to transform the input graph in a set of trees, one for each connected component in the graph. Nodes are iteratively removed from
the graph and added to the trees, reducing the amount of computation at each iteration. We prove the correctness of the algorithm,
evaluate its computational cost and provide an extensive experimental evaluation considering a wide variety of synthetic and real-world
graphs. The experimental results show that CRACKER consistently outperforms state-of-the-art approaches both in terms of total
computation time and volume of messages exchanged.


### Publications

**2016 - IEEE Transaction on Parallel and Distributed Systems**

Lulli, Alessandro, et al. 
**Fast Connected Components Computation in Large Graphs by Vertex Pruning.** 
IEEE Transactions on parallel and distributed systems (2016) (to appear).

@article{lulli2016fast,
  title={Fast Connected Components Computation in Large Graphs by Vertex Pruning},
  author={Lulli, Alessandro and Carlini, Emanuele and Dazzi, Patrizio and Lucchese, Claudio and Ricci, Laura},
  journal={IEEE Transactions on parallel and distributed systems},
  year={2016},
  publisher={IEEE}
}

**2015 - IEEE Symposium on Computers and Communication (ISCC)**

Lulli, Alessandro, et al. 
**Cracker: Crumbling large graphs into connected components.** 
2015 IEEE Symposium on Computers and Communication (ISCC). IEEE, 2015.

@inproceedings{lulli2015cracker,
  title={Cracker: Crumbling large graphs into connected components},
  author={Lulli, Alessandro and Ricci, Laura and Carlini, Emanuele and Dazzi, Patrizio and Lucchese, Claudio},
  booktitle={2015 IEEE Symposium on Computers and Communication (ISCC)},
  pages={574--581},
  year={2015},
  organization={IEEE}
}

### How to build

mvn clean package

### How to run

spark-submit --class util.Main --executor-cores <#core> --driver-memory <#memory>g --master spark://<your spark master>:7077 target/cracker-0.0.1-SNAPSHOT.jar CRACKER config_example

