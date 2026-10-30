(ns adam.replica.processing-test
  (:require [adam.replica.jsonl :as jsonl]
            [adam.replica.processing :as processing]
            [cljs.test :refer [deftest is testing]]
            [clojure.string :as string]
            [clojure.test.check :as tc]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]))

(def lines
  ["{\"type\":\"session\",\"id\":\"s\",\"cwd\":\"/repo\"}"
   "{\"type\":\"message\",\"id\":\"a\",\"parentId\":null,\"message\":{\"role\":\"assistant\",\"content\":[{\"type\":\"toolCall\",\"name\":\"read\",\"id\":\"call\",\"arguments\":{\"path\":\"src/a.cljs\"}}]}}"
   "{\"type\":\"message\",\"id\":\"b\",\"parentId\":\"a\",\"message\":{\"role\":\"toolResult\",\"toolCallId\":\"call\"}}"])

(defn records [values]
  (mapv #(hash-map :bytes (js/Buffer.from % "utf8") :newline? true) values))

(defn process-batches [batches]
  (let [state (:state (processing/advance nil {:op :start :path "input.jsonl"}))
        state (reduce (fn [state batch]
                        (:state (processing/advance state {:op :records :records batch})))
                      state batches)]
    (let [finished (processing/advance state {:op :finish :current-leaf-id "b"})]
      (processing/advance (:state finished) {:op :prepare}))))

(deftest processing-events-are-data-only-and-partition-independent
  (let [all (records lines)
        whole (process-batches [all])
        split (process-batches (mapv vector all))]
    (is (= (:result whole) (:result split)))
    (is (= 2 (get-in whole [:result :summary :entry-count])))
    (is (= [{:cwd "/repo" :path "src/a.cljs"}] (get-in whole [:result :file-locators])))
    (is (= "input.jsonl" (get-in whole [:result :summary :path])))
    (is (= (:log-hash (:summary (:result whole)))
           (-> (js/require "node:crypto") (.createHash "sha256")
               (.update (str (string/join "\n" lines) "\n")) (.digest "hex"))))
    (is (true? (:pass? (tc/quick-check 40
       (prop/for-all [width (gen/choose 1 6)]
         (= (:result whole) (:result (process-batches (partition-all width all)))))
       :seed 3601))))))

(deftest a-not-yet-persisted-selected-leaf-never-becomes-an-empty-valid-projection
  (let [initial (:state (processing/advance nil {:op :start :path "input.jsonl"}))
        state (:state (processing/advance initial {:op :records :records (records lines)}))]
    (is (thrown-with-msg? js/Error #"not persisted"
                         (processing/advance state {:op :finish :current-leaf-id "missing"})))
    (is (= 2 (get-in (processing/advance state {:op :finish :current-leaf-id nil})
                     [:result :summary :entry-count])))))

(deftest processing-plans-preserve-null-selection-and-invalid-inputs
  (doseq [[name input] [["duplicate" [(first lines) (second lines) (second lines)]]
                       ["missing parent" [(first lines) "{\"type\":\"message\",\"id\":\"x\",\"parentId\":\"missing\"}"]]
                       ["malformed" [(first lines) "!"]]]]
    (testing name
      (is (thrown? js/Error (process-batches [(records input)])))))
  (let [ready (:state (process-batches [(records lines)]))
        {:keys [state result]} (processing/advance ready {:op :plan :user-uuid "u" :current-leaf-id nil})
        batch (:result (processing/advance state {:op :batch :index 0}))]
    (is (= :pending (:status result)))
    (is (nil? (get-in result [:session :current-leaf-id])))
    (is (= ["a" "b"] (mapv :entry-id (:entries batch))))
    (is (= 1 (count (:batches (:plan state)))))
    (is (not (contains? result :batches)) "large payloads are retrieved one bounded batch at a time")))
