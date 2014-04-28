(ns backtype.storm.blueprint
  ^{:author ecsark}
  (:require [clojure [string :as str]])
  (:import [clojure.lang RT]
           [backtype.storm.generated InvalidTopologyException StormTopology])
  (:use [backtype.storm log]))

(defn parse-blueprint [blueprint]
  (try
    (let [cluster (clojure.string/split blueprint #";")]
      (->> cluster
        (map (fn [supervisor]
               (map (fn [executor]
                      (map (fn [task]
                             (let [pair (clojure.string/split task #":")]
                               [(first pair) (Integer/parseInt (last pair))]))
                        (clojure.string/split executor #"&")))
                 (clojure.string/split supervisor #"[,]"))))))
    (catch Exception ex
      (throw (InvalidTopologyException. (str "Blueprint format error: " blueprint))))))


(defn partition-blueprint [parsed-blueprint component->task]
  "Compute task->executor according to the user-given blueprint.
  Each system-added component will be granted a single executor.
  If blueprint contains components not found in the topology,
  or assigns more tasks than those shown in the topology, exception will be raised.
  But tasks in the topology, if not assigned in the blueprint, will be eliminated by this function."
  (let [roster (atom component->task)
        user-tasks (apply concat
                     (for [node parsed-blueprint]
                       (apply concat
                         (for [worker node]
                           (for [executor worker]
                             (try
                              (let [task-name (first executor)
                                   task-num (last executor)
                                   task-id (first (get @roster task-name))
                                   executor-assign [task-id (- (+ task-id task-num) 1)]
                                   _ (swap! roster (fn [alstate] (update-in alstate [task-name] #(subvec % task-num))))]
                               executor-assign)
                               (catch Exception e
                                 (throw (InvalidTopologyException. (str "Inconsistent number of tasks "
                                                                     "between topology and blueprint."))))))))))
        system-tasks (->> parsed-blueprint
                       flatten
                       (filter #(false? (number? %)))
                       distinct
                       (apply dissoc component->task)
                       vals
                       (apply concat)
                       (map #(into [] (repeat 2 %))))]
  (concat user-tasks system-tasks)))


(defn seq-contains? [coll target] (some #(= target %) coll))

(defn seq-not-contains? [coll target] (not-any? #(= target %) coll))

(defn dummy-map [coll] (->> coll (map (fn [s] [s []])) (apply concat) (apply hash-map)))


;;return operator-set: bolt->rest-bolts-in-set
(defn parse-operator-set [^StormTopology topology]
  (let [stateless-bolts->upstreams (->> (for [[k v] (.get_state_bolts topology)]
                          [k (->> v .get_common .get_inputs keys
                            (map #(.get_componentId %)) (apply vector))])
                          (apply concat) (apply hash-map))
        stateful-bolts->upstreams (->> (for [[k v] (.get_bolts topology)]
                          [k (->> v .get_common .get_inputs keys
                            (map #(.get_componentId %)) (apply vector))])
                         (apply concat) (apply hash-map))

        spouts->dummy (dummy-map (keys (.get_spouts topology)))

        stateless-bolts (keys stateless-bolts->upstreams)

        all-bolts->upstreams (merge stateless-bolts->upstreams stateful-bolts->upstreams)

        stless-bolts->downstream-closure (atom (dummy-map stateless-bolts))

        _ (doseq [the-bolt stateless-bolts]
            (doseq [upstream (all-bolts->upstreams the-bolt)]
              (if (seq-contains? stateless-bolts upstream)
                (swap! stless-bolts->downstream-closure
                  (fn [x]
                    (update-in x [upstream]
                      #(-> % (conj the-bolt) (distinct))))))))

        _ (dotimes [n (count stateless-bolts)]
            (doseq [the-bolt stateless-bolts]
              (doseq [downstream (@stless-bolts->downstream-closure the-bolt)]
                (swap! stless-bolts->downstream-closure
                  (fn [x]
                    (update-in x [the-bolt]
                      #(->> % (apply conj (x downstream)) (distinct))))))))

        multi-operator (->> (for [[the-bolt upstreams] (select-keys all-bolts->upstreams stateless-bolts)]
                            (->> upstreams
                              (filter #(seq-not-contains? stateless-bolts %))
                              (map #(if (contains? spouts->dummy %)
                                ;; if the upstream is a spout, create an operator set with the leading
                                ;; stateless bolt as the key, and the following as value
                                      (vector (@stless-bolts->downstream-closure the-bolt) the-bolt)
                                      (vector % (conj (@stless-bolts->downstream-closure the-bolt) the-bolt))))
                              (apply concat)))
                       (apply concat)
                       (apply hash-map))

        single-operator (->> (keys all-bolts->upstreams)
                          (filter #(seq-not-contains? (keys multi-operator) %))
                          (filter #(seq-not-contains? stateless-bolts %))
                          dummy-map)]

    (merge multi-operator single-operator spouts->dummy))
)