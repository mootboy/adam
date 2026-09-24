(ns adam.replica.sync-test
  (:require [adam.replica.sync :as sync]
            [cljs.test :refer [deftest is]]))

(def summary
  {:entry-count 4
   :log-hash "full-hash"
   :header-next-byte-offset 50
   :header-prefix-hash "header-hash"})

(def entries
  [{:entry-id "a" :ordinal 0 :payload-bytes 3 :next-byte-offset 60 :prefix-hash "hash-a"}
   {:entry-id "b" :ordinal 1 :payload-bytes 3 :next-byte-offset 70 :prefix-hash "hash-b"}
   {:entry-id "c" :ordinal 2 :payload-bytes 10 :next-byte-offset 90 :prefix-hash "hash-c"}
   {:entry-id "d" :ordinal 3 :payload-bytes 2 :next-byte-offset 100 :prefix-hash "hash-d"}])

(deftest plans-only-the-appended-suffix-in-byte-bounded-batches
  (let [checkpoint {:complete-through-ordinal 0
                    :complete-through-byte-offset 60
                    :committed-prefix-hash "hash-a"}
        plan (sync/plan-sync summary entries checkpoint 6)]
    (is (= :pending (:status plan)))
    (is (= [["b"] ["c"] ["d"]]
           (mapv #(mapv :entry-id (:entries %)) (:batches plan))))
    (is (= ["hash-b" "hash-c" "hash-d"]
           (mapv #(get-in % [:checkpoint :committed-prefix-hash])
                 (:batches plan))))))

(deftest detects-checkpoint-divergence-before-planning-writes
  (let [plan (sync/plan-sync
              summary
              entries
              {:complete-through-ordinal 1
               :complete-through-byte-offset 70
               :committed-prefix-hash "different"}
              100)]
    (is (= {:status :conflict
            :reason :prefix-mismatch
            :expected-hash "different"
            :actual-hash "hash-b"}
           plan))))

(deftest recognizes-an-unchanged-completed-log
  (is (= {:status :unchanged :batches []}
         (sync/plan-sync
          summary
          entries
          {:complete-through-ordinal 3
           :complete-through-byte-offset 100
           :committed-prefix-hash "hash-d"
           :entry-count 4
           :log-hash "full-hash"}
          100))))
