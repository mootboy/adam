(ns adam.knowledge.memory-test
  (:require [adam.knowledge.memory :as memory]
            [cljs.test :refer-macros [deftest is testing]]))

(defn- sidecar-record [id producer kind fields]
  {:id (str "record-" id)
   :semantic-status :accepted
   :raw-json
   (js/JSON.stringify
    (clj->js
     (merge {:protocolVersion 1
             :eventId (str "event-" id)
             :kind kind
             :recordedAt "2026-01-01T00:00:00.000Z"
             :producer {:id producer :version "1.0.0"}
             :source {:kind "claude-code" :sessionId "session-1"}
             :sourceCheckpoint
             {:streams [{:streamId "main" :committedBytes 10
                         :prefixSha256 (apply str (repeat 64 "a"))
                         :selectedLeafEntryId "entry-1"}]}}
            fields)))})

(deftest aggregates-producer-local-memory-with-stream-qualified-citations
  (let [projection
        (memory/aggregate-projection
         {:source-kind "claude-code"
          :source-session-id "session-1"
          :embedded-projections []
          :streams
          [{:producer-id "producer-a"
            :records
            [(sidecar-record
              "a-observation" "producer-a" "observations.recorded"
              {:observations
               [{:id "aaaaaaaaaaaa" :content "first" :timestamp "2026-01-01T00:00:00.000Z"
                 :relevance "high" :tokenCount 1
                 :sourceEntries [{:streamId "main" :entryId "entry-1"}]}]})
             (sidecar-record
              "a-reflection" "producer-a" "reflections.recorded"
              {:reflections
               [{:id "bbbbbbbbbbbb" :content "support" :tokenCount 1
                 :supportingObservationIds ["aaaaaaaaaaaa"]}]})]}
           {:producer-id "producer-b"
            :records
            [(sidecar-record
              "b-drop" "producer-b" "observations.dropped"
              {:observationIds ["aaaaaaaaaaaa"]})
             (sidecar-record
              "b-observation" "producer-b" "observations.recorded"
              {:observations
               [{:id "aaaaaaaaaaaa" :content "second" :timestamp "2026-01-01T00:00:01.000Z"
                 :relevance "medium" :tokenCount 1
                 :sourceEntries [{:streamId "agent:one" :entryId "entry-2"}]}]})]}]})
        observations (:observations projection)]
    (is (= [["producer-a" "aaaaaaaaaaaa" false]
            ["producer-b" "aaaaaaaaaaaa" true]]
           (mapv (juxt :producer :memory-id :dropped?) observations)))
    (is (= [{:stream-id "main" :entry-id "entry-1"}]
           (:source-entries (first observations))))
    (is (= "producer-a" (get-in projection [:reflections 0 :producer])))
    (is (= ["aaaaaaaaaaaa"]
           (get-in projection [:reflections 0 :supporting-observation-ids])))))

(deftest retains-first-definition-and-folds-only-accepted-records
  (let [first-definition
        (sidecar-record
         "first" "producer-a" "observations.recorded"
         {:observations
          [{:id "aaaaaaaaaaaa" :content "first" :timestamp "2026-01-01T00:00:00.000Z"
            :relevance "high" :tokenCount 1
            :sourceEntries [{:streamId "main" :entryId "entry-1"}]}]})
        conflicting-definition
        (sidecar-record
         "second" "producer-a" "observations.recorded"
         {:observations
          [{:id "aaaaaaaaaaaa" :content "changed" :timestamp "2026-01-01T00:00:01.000Z"
            :relevance "low" :tokenCount 1
            :sourceEntries [{:streamId "main" :entryId "entry-2"}]}]})
        blocked (assoc conflicting-definition :id "record-blocked" :semantic-status :blocked)
        projection
        (memory/aggregate-projection
         {:source-kind "claude-code" :source-session-id "session-1"
          :embedded-projections []
          :streams [{:producer-id "producer-a"
                     :records [first-definition conflicting-definition blocked]}]})]
    (is (= ["first"] (mapv :content (:observations projection))))
    (is (= [:conflicting-memory-definition]
           (mapv :reason (:diagnostics projection))))))

(deftest bounds-aggregate-diagnostics
  (let [projection
        (memory/aggregate-projection
         {:source-kind "pi" :source-session-id "session-1"
          :embedded-projections
          [{:diagnostics (mapv (fn [index] {:reason :invalid :index index})
                               (range 150))}]
          :streams []})]
    (is (= 100 (count (:diagnostics projection))))))

(deftest includes-embedded-pi-memory-in-the-same-snapshot
  (let [projection
        (memory/aggregate-projection
         {:source-kind "pi" :source-session-id "pi-session"
          :embedded-projections
          [{:producer "pi-observational-memory" :adapter-version 1
            :observations [{:producer "pi-observational-memory" :adapter-version 1
                            :memory-id "aaaaaaaaaaaa" :content "embedded"
                            :timestamp "2026-01-01T00:00:00.000Z" :relevance "high"
                            :token-count 1 :recording-entry-id "memory-entry"
                            :source-entry-ids ["source-entry"] :dropped? false}]
            :reflections [] :diagnostics []}]
          :streams []})]
    (is (= "embedded" (get-in projection [:observations 0 :content])))
    (is (= [{:stream-id "main" :entry-id "source-entry"}]
           (get-in projection [:observations 0 :source-entries])))
    (is (= "memory-entry" (get-in projection [:observations 0 :recording-entry-id])))
    (is (re-matches #"pi-[a-f0-9]{64}"
                    (get-in projection [:observations 0 :recording-event-id])))))
