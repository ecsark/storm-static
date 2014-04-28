;; Licensed to the Apache Software Foundation (ASF) under one
;; or more contributor license agreements.  See the NOTICE file
;; distributed with this work for additional information
;; regarding copyright ownership.  The ASF licenses this file
;; to you under the Apache License, Version 2.0 (the
;; "License"); you may not use this file except in compliance
;; with the License.  You may obtain a copy of the License at
;;
;; http://www.apache.org/licenses/LICENSE-2.0
;;
;; Unless required by applicable law or agreed to in writing, software
;; distributed under the License is distributed on an "AS IS" BASIS,
;; WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
;; See the License for the specific language governing permissions and
;; limitations under the License.


(ns backtype.storm.scheduler.EasyScheduler
  (:use [backtype.storm util log config]
        [backtype.storm.daemon common])
  (:require [clojure.set :as set]
            [clojure.string :as string])
  (:import [backtype.storm.scheduler IScheduler Topologies
            Cluster TopologyDetails WorkerSlot ExecutorDetails]
           [backtype.storm.generated InvalidTopologyException])
  (:use [backtype.storm blueprint])
  (:gen-class
    :implements [backtype.storm.scheduler.IScheduler]))

(defn sort-slots [all-slots]
  (let [split-up (vals (group-by first all-slots))]
    (apply interleave-all split-up)
    ))

(defn get-alive-assigned-node+port->executors [cluster topology-id]
  (let [existing-assignment (.getAssignmentById cluster topology-id)
        executor->slot (if existing-assignment
                         (.getExecutorToSlot existing-assignment)
                         {})
        executor->node+port (into {} (for [[^ExecutorDetails executor ^WorkerSlot slot] executor->slot
                                           :let [executor [(.getStartTask executor) (.getEndTask executor)]
                                                 node+port [(.getNodeId slot) (.getPort slot)]]]
                                       {executor node+port}))
        alive-assigned (reverse-map executor->node+port)]
    alive-assigned))


(defn- schedule-fall-back [^TopologyDetails topology ^Cluster cluster]
  (let [topology-id (.getId topology)
        available-slots (->> (.getAvailableSlots cluster)
                          (map #(vector (.getNodeId %) (.getPort %))))
        all-executors (->> topology
                        .getExecutors
                        (map #(vector (.getStartTask %) (.getEndTask %)))
                        set)
        alive-assigned (get-alive-assigned-node+port->executors cluster topology-id)
        total-slots-to-use (min (.getNumWorkers topology)
                             (+ (count available-slots) (count alive-assigned)))
        reassign-slots (take (- total-slots-to-use (count alive-assigned))
                         (sort-slots available-slots))
        reassign-executors (sort (set/difference all-executors (set (apply concat (vals alive-assigned)))))
        reassignment (into {}
                       (map vector
                         reassign-executors
                         ;; for some reason it goes into infinite loop without limiting the repeat-seq
                         (repeat-seq (count reassign-executors) reassign-slots)))]
    (when-not (empty? reassignment)
      (log-message "Available slots: " (pr-str available-slots))
      )
    reassignment))


;;TODO dealing with dead slots
(defn- schedule-topology [^TopologyDetails topology ^Cluster cluster]
  (let [topology-conf (.getConf topology)
         blueprint (.get topology-conf "blueprint")]
    (if (not-nil? blueprint)
        (let [task->component (storm-task-info (.getTopology topology) topology-conf)
              ;;it is kindof hack. compute task->executor again, with orders!!
              user-plan (->> blueprint
                          parse-blueprint
                          (sort-by count)
                          reverse) ;; order by number of workers (translating into processes) on the node
              supervisor-num (->> user-plan (map #(count %)))
              executors-in-supervisor (->> user-plan
                                        (map (fn [supervisor] (reduce +
                                             (map (fn [executor] (count executor)))))))
              executor-partition (flatten (->> user-plan
                                                  (map (fn [supervisor]
                                                         (map (fn [executor] (count executor)) supervisor)))))
              assign-tasks (partition-blueprint user-plan (reverse-map task->component))
              available-slots-by-node (->> (.getAvailableSlots cluster)
                                (map #(vector (.getNodeId %) (.getPort %)))
                                (group-by first)
                                (into [])
                                (sort-by count)
                                reverse)
              _ (if (< (count available-slots-by-node) (count user-plan))
                  (throw (InvalidTopologyException. (str "Plan should include no more than "
                                                      (count available-slots-by-node) " nodes"))))
              available-slots (apply concat (vals available-slots-by-node))
              useful-slots (apply concat (for [[exe-num avail] (map list supervisor-num available-slots-by-node)]
                               (take exe-num (val avail))))
              _ (if (< (count useful-slots) (count executor-partition))
                  (throw (InvalidTopologyException. (str "Too much workers on a node. Available slots: ["
                                                      (string/join ", "
                                                        (map #(count (last %)) available-slots-by-node)) "]"))))
              unuseful-slots (reduce disj (set available-slots) (set useful-slots))
              assign-slots (apply concat (for [[num-executor slot] (map list executor-partition useful-slots)]
                               (repeat num-executor slot)))
              ;; for simplicity, the extra tasks are projected to a first founded unused slot
              assign-slots-extra (repeat (- (count assign-tasks) (count assign-slots)) (first unuseful-slots))
              assign-slots-all (concat assign-slots assign-slots-extra)
              assignment (into {} (map vector assign-tasks assign-slots-all))
              _ (log-message (str "EasyScheduler computes the assignment: " (print-str assignment)))
              ]
          assignment
          )
  (schedule-fall-back topology cluster)))
  )


(defn schedule-topologies-easily [^Topologies topologies ^Cluster cluster]
  (let [needs-scheduling-topologies (.needsSchedulingTopologies cluster topologies)]
    (doseq [^TopologyDetails topology needs-scheduling-topologies
            :let [topology-id (.getId topology)
                  new-assignment (schedule-topology topology cluster)
                  node+port->executors (reverse-map new-assignment)]]
      (doseq [[node+port executors] node+port->executors
              :let [^WorkerSlot slot (WorkerSlot. (first node+port) (last node+port))
                    executors (for [[start-task end-task] executors]
                                (ExecutorDetails. start-task end-task))]]
        (.assign cluster slot topology-id executors)))))

(defn -prepare [this conf]
  )

(defn -schedule [this ^Topologies topologies ^Cluster cluster]
  (schedule-topologies-easily topologies cluster))
