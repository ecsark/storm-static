(ns starter.word-count-topology
  ^{:author ecsark}
  (:use [clojure test])
  (:import [backtype.storm.topology TopologyBuilder]
           [starter WordCount SplitSentence RandomSentenceSpout]
           [backtype.storm Config LocalCluster]
           [backtype.storm.tuple Fields])

  )

(defn run []
  (let
    [builder (TopologyBuilder.)
        spout (-> builder
                (.setSpout "spout" (RandomSentenceSpout.) 1)
                (.setNumTasks 1)
                )
        bolt-split (-> builder
                     (.setBolt "split" (SplitSentence.) 2)
                     (.setNumTasks 3)
                     (.shuffleGrouping "spout")
                     )
        bolt-split (-> builder
                     (.setStateBolt "split2" (SplitSentence.))
                     (.setNumTasks 3)
                     (.shuffleGrouping "split")
                     )
        bolt-count (-> builder
                     (.setBolt  "count" (WordCount.) 3)
                     (.setNumTasks 6)
                     (.fieldsGrouping "split2" (Fields. ["word"]))
                     )

        conf (Config.)
        cluster (LocalCluster.)]
    (.setDebug conf true)
    ;;(.setMaxTaskParallelism conf 3)
    (.put conf "blueprint" "spout:1&split:2,split:1,count:1&count:2,count:3")
    ()
    (.setNumWorkers conf 2)
    (.submitTopology cluster "word-newcount" conf (.createTopology builder))

    (Thread/sleep 7000)
    (.shutdown cluster)

    )

  )

(run)
