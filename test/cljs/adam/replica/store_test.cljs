(ns adam.replica.store-test
  (:require [adam.replica.store :as store]
            [cljs.test :refer [deftest is]]))

(deftest immutable-entry-conflicts-have-structured-diagnostics
  (let [existing {:entry-id "entry-1" :payload-hash "old-hash"}
        incoming {:entry-id "entry-1" :payload-hash "new-hash"}
        error (try
                (store/assert-entry-compatible! existing incoming)
                nil
                (catch :default error error))]
    (is (store/immutable-entry-conflict? error))
    (is (= {:type :immutable-entry-conflict
            :entry-id "entry-1"
            :expected-hash "old-hash"
            :actual-hash "new-hash"}
           (ex-data error)))))

(deftest matching-entry-payloads-are-idempotent
  (let [entry {:entry-id "entry-1" :payload-hash "same-hash"}]
    (is (= entry (store/assert-entry-compatible! entry entry)))))
