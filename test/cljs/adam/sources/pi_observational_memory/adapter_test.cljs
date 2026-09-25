(ns adam.sources.pi-observational-memory.adapter-test
  (:require [adam.sources.pi-observational-memory.adapter :as adapter]
            [cljs.test :refer [deftest is testing]]))

(defn- stored [entry]
  {:entry-id (aget entry "id")
   :raw-json (js/JSON.stringify entry)})

(defn- custom-entry [id custom-type data]
  #js {:type "custom" :id id :parentId nil
       :customType custom-type :data data})

(def valid-observation
  #js {:id "aaaaaaaaaaaa"
       :content "The implementation reads the selected file."
       :timestamp "2026-01-01T00:00:00.000Z"
       :relevance "high"
       :sourceEntryIds #js ["result-1"]
       :tokenCount 7})

(def valid-reflection
  #js {:id "bbbbbbbbbbbb"
       :content "Keep the deterministic path boundary"
       :supportingObservationIds #js ["aaaaaaaaaaaa"]
       :tokenCount 5})

(deftest extracts-provider-neutral-memories-and-dropped-state
  (let [projection
        (adapter/extract-memories
         adapter/adapter
         [(stored (custom-entry
                   "memory-1" "om.observations.recorded"
                   #js {:observations #js [valid-observation]
                        :coversUpToId "result-1"}))
          (stored (custom-entry
                   "memory-2" "om.reflections.recorded"
                   #js {:reflections #js [valid-reflection]
                        :coversUpToId "memory-1"}))
          (stored (custom-entry
                   "drop-1" "om.observations.dropped"
                   #js {:observationIds #js ["aaaaaaaaaaaa"]
                        :coversUpToId "memory-2"}))])]
    (is (= "pi-observational-memory" (:producer projection)))
    (is (= 1 (:adapter-version projection)))
    (is (= [{:producer "pi-observational-memory"
             :adapter-version 1
             :memory-id "aaaaaaaaaaaa"
             :content "The implementation reads the selected file."
             :timestamp "2026-01-01T00:00:00.000Z"
             :relevance "high"
             :token-count 7
             :recording-entry-id "memory-1"
             :source-entry-ids ["result-1"]
             :dropped? true}]
           (:observations projection)))
    (is (= ["aaaaaaaaaaaa"]
           (:supporting-observation-ids (first (:reflections projection)))))
    (is (empty? (:diagnostics projection)))))

(deftest preserves-first-valid-record-and-ignores-unrelated-custom-entries
  (let [duplicate (js/Object.assign #js {} valid-observation
                                    #js {:content "later duplicate"})
        projection
        (adapter/extract-memories
         adapter/adapter
         [(stored (custom-entry "one" "unrelated.entry" #js {:value true}))
          (stored (custom-entry "memory-1" "om.observations.recorded"
                                #js {:observations #js [valid-observation]
                                     :coversUpToId "source"}))
          (stored (custom-entry "memory-2" "om.observations.recorded"
                                #js {:observations #js [duplicate]
                                     :coversUpToId "source"}))])]
    (is (= 1 (count (:observations projection))))
    (is (= "The implementation reads the selected file."
           (:content (first (:observations projection)))))
    (is (empty? (:diagnostics projection)))))

(deftest isolates-malformed-and-unsupported-producer-entries
  (testing "malformed supported entries report structural diagnostics without content"
    (let [projection
          (adapter/extract-memories
           adapter/adapter
           [(stored (custom-entry
                     "bad-1" "om.observations.recorded"
                     #js {:observations #js [#js {:id "not-a-memory-id"
                                                  :content "do not report me"}]
                          :coversUpToId "source"}))
            (stored (custom-entry
                     "future-1" "om.reflections.recorded"
                     #js {:schemaVersion 99
                          :reflections #js [valid-reflection]
                          :coversUpToId "source"}))])]
      (is (empty? (:observations projection)))
      (is (empty? (:reflections projection)))
      (is (= [{:producer "pi-observational-memory"
               :entry-id "bad-1" :reason :malformed-entry}
              {:producer "pi-observational-memory"
               :entry-id "future-1" :reason :unsupported-schema-version}]
             (:diagnostics projection)))
      (is (not (re-find #"do not report me" (pr-str (:diagnostics projection))))))))
