(ns adam.replica.sync-test
  (:require [adam.replica.store :as store]
            [adam.replica.sync :as sync]
            [cljs.test :refer [async deftest is]]))

(defrecord FakeReplica [checkpoint writes completions conflicts]
  store/SessionReplicaStore
  (initialize! [_ _] (js/Promise.resolve nil))
  (get-checkpoint! [_ _] (js/Promise.resolve checkpoint))
  (write-batch! [_ request]
    (swap! writes conj request)
    (js/Promise.resolve nil))
  (complete-session! [_ request]
    (swap! completions conj request)
    (js/Promise.resolve nil))
  (mark-conflict! [_ conflict]
    (swap! conflicts conj conflict)
    (js/Promise.resolve nil))
  (list-sessions! [_ _] (js/Promise.resolve []))
  (read-session! [_ _] (js/Promise.resolve nil))
  (close! [_] (js/Promise.resolve nil)))

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

(deftest synchronizes-planned-batches-sequentially-through-the-store-protocol
  (async done
    (let [writes (atom [])
          completions (atom [])
          conflicts (atom [])
          replica (->FakeReplica nil writes completions conflicts)
          complete-summary (assoc summary
                                  :pi-session-id "session-1"
                                  :header-json "{\"type\":\"session\"}"
                                  :log-bytes 100
                                  :largest-entry-bytes 10
                                  :has-final-newline? true)]
      (-> (sync/sync-scanned-session!
           {:summary complete-summary
            :entries entries
            :user-uuid "user-1"
            :source-file "/sessions/session.jsonl"
            :replica replica
            :batch-bytes 6})
          (.then
           (fn [result]
             (is (= {:status :mirrored :entries-written 4 :batches-written 3}
                    result))
             (is (= [["a" "b"] ["c"] ["d"]]
                    (mapv #(mapv :entry-id (:entries %)) @writes)))
             (is (= 1 (count @completions)))
             (is (empty? @conflicts))
             (done)))
          (.catch
           (fn [error]
             (is false (.-stack error))
             (done)))))))

(deftest records-prefix-conflicts-without-writing
  (async done
    (let [writes (atom [])
          completions (atom [])
          conflicts (atom [])
          checkpoint {:complete-through-ordinal 0
                      :complete-through-byte-offset 60
                      :committed-prefix-hash "different"}
          replica (->FakeReplica checkpoint writes completions conflicts)
          complete-summary (assoc summary
                                  :pi-session-id "session-1"
                                  :header-json "{\"type\":\"session\"}"
                                  :log-bytes 100
                                  :largest-entry-bytes 10
                                  :has-final-newline? true)]
      (-> (sync/sync-scanned-session!
           {:summary complete-summary
            :entries entries
            :user-uuid "user-1"
            :source-file "/sessions/session.jsonl"
            :replica replica})
          (.then
           (fn [result]
             (is (= :conflict (:status result)))
             (is (= :prefix-mismatch (get-in result [:conflict :reason])))
             (is (empty? @writes))
             (is (empty? @completions))
             (is (= 1 (count @conflicts)))
             (done)))
          (.catch
           (fn [error]
             (is false (.-stack error))
             (done)))))))
