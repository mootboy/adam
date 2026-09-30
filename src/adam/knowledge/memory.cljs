(ns adam.knowledge.memory
  (:require [adam.knowledge.store :as store]
            [adam.memory.protocol :as protocol]
            [clojure.string :as string]))

(def ^:private pi-implicit-stream "main")
(def ^:private max-diagnostics 100)

(defn- json-object? [value]
  (and (some? value)
       (= "object" (goog/typeOf value))
       (not (js/Array.isArray value))))

(defn- parse-accepted-event [stream record]
  (when (= :accepted (:semantic-status record))
    (try
      (let [value (js/JSON.parse (:raw-json record))
            producer (when (json-object? (aget value "producer"))
                       (aget (aget value "producer") "id"))
            source (aget value "source")]
        (when (and (json-object? value)
                   (= (:producer-id stream) producer)
                   (json-object? source))
          {:value value
           :producer producer
           :producer-version (aget (aget value "producer") "version")
           :source-kind (aget source "kind")
           :source-session-id (aget source "sessionId")
           :event-id (aget value "eventId")
           :record-id (:id record)}))
      (catch :default _ nil))))

(defn- source-entries [values]
  (mapv (fn [value]
          {:stream-id (aget value "streamId")
           :entry-id (aget value "entryId")})
        (array-seq values)))

(defn- sidecar-observation [event value]
  {:producer (:producer event)
   :producer-version (:producer-version event)
   :memory-id (aget value "id")
   :content (aget value "content")
   :timestamp (aget value "timestamp")
   :relevance (aget value "relevance")
   :token-count (aget value "tokenCount")
   :recording-event-id (:event-id event)
   :recording-memory-record-id (:record-id event)
   :source-entries (source-entries (aget value "sourceEntries"))})

(defn- sidecar-reflection [event value]
  {:producer (:producer event)
   :producer-version (:producer-version event)
   :memory-id (aget value "id")
   :content (aget value "content")
   :token-count (aget value "tokenCount")
   :recording-event-id (:event-id event)
   :recording-memory-record-id (:record-id event)
   :supporting-observation-ids
   (vec (array-seq (aget value "supportingObservationIds")))})

(defn- definition-equal? [kind left right]
  (let [fields (case kind
                 :observation
                 [:producer :memory-id :content :timestamp :relevance
                  :token-count :source-entries]
                 :reflection
                 [:producer :memory-id :content :token-count
                  :supporting-observation-ids])]
    (= (select-keys left fields) (select-keys right fields))))

(defn- add-diagnostic! [diagnostics diagnostic]
  (when (< (count @diagnostics) max-diagnostics)
    (swap! diagnostics conj diagnostic)))

(defn- add-definition! [definitions order diagnostics kind memory]
  (let [key [(:producer memory) (:memory-id memory)]]
    (if-let [existing (get @definitions key)]
      (when-not (definition-equal? kind existing memory)
        (add-diagnostic! diagnostics
                         {:producer (:producer memory)
                          :memory-id (:memory-id memory)
                          :kind kind
                          :reason :conflicting-memory-definition}))
      (do
        (swap! definitions assoc key memory)
        (swap! order conj key)))))

(defn- fold-sidecar-event!
  [observations observation-order reflections reflection-order dropped diagnostics event]
  (let [value (:value event)]
    (case (aget value "kind")
      "observations.recorded"
      (doseq [item (array-seq (aget value "observations"))]
        (add-definition! observations observation-order diagnostics :observation
                         (sidecar-observation event item)))

      "reflections.recorded"
      (doseq [item (array-seq (aget value "reflections"))]
        (add-definition! reflections reflection-order diagnostics :reflection
                         (sidecar-reflection event item)))

      "observations.dropped"
      (doseq [memory-id (array-seq (aget value "observationIds"))]
        (swap! dropped conj [(:producer event) memory-id]))

      nil)))

(defn- synthetic-event-id
  [user-id source-kind source-session-id producer recording-entry-id]
  (str "pi-" (protocol/sha256
              (string/join "\u0000" [user-id source-kind source-session-id
                                      producer recording-entry-id]))))

(defn- embedded-observation
  [user-id source-kind source-session-id projection memory]
  (let [producer (or (:producer memory) (:producer projection))]
    (assoc memory
           :producer producer
           :adapter-version (or (:adapter-version memory)
                                (:adapter-version projection))
           :recording-event-id
           (synthetic-event-id user-id source-kind source-session-id producer
                               (:recording-entry-id memory))
           :source-entries
           (mapv (fn [entry-id]
                   {:stream-id pi-implicit-stream :entry-id entry-id})
                 (:source-entry-ids memory)))))

(defn- embedded-reflection
  [user-id source-kind source-session-id projection memory]
  (let [producer (or (:producer memory) (:producer projection))]
    (assoc memory
           :producer producer
           :adapter-version (or (:adapter-version memory)
                                (:adapter-version projection))
           :recording-event-id
           (synthetic-event-id user-id source-kind source-session-id producer
                               (:recording-entry-id memory)))))

(defn aggregate-projection
  [{:keys [user-id source-kind source-session-id embedded-projections streams]}]
  (let [observations (atom {})
        observation-order (atom [])
        reflections (atom {})
        reflection-order (atom [])
        dropped (atom #{})
        diagnostics (atom [])]
    (doseq [projection embedded-projections]
      (doseq [diagnostic (:diagnostics projection)]
        (add-diagnostic! diagnostics diagnostic))
      (doseq [memory (:observations projection)]
        (let [memory (embedded-observation user-id source-kind source-session-id
                                           projection memory)]
          (add-definition! observations observation-order diagnostics :observation memory)
          (when (:dropped? memory)
            (swap! dropped conj [(:producer memory) (:memory-id memory)]))))
      (doseq [memory (:reflections projection)]
        (add-definition! reflections reflection-order diagnostics :reflection
                         (embedded-reflection user-id source-kind source-session-id
                                              projection memory))))
    (doseq [stream (sort-by :producer-id streams)
            record (sort-by :ordinal (:records stream))]
      (if-let [event (parse-accepted-event stream record)]
        (if (and (= source-kind (:source-kind event))
                 (= source-session-id (:source-session-id event)))
          (fold-sidecar-event! observations observation-order reflections
                               reflection-order dropped diagnostics event)
          (add-diagnostic! diagnostics
                           {:producer (:producer-id stream)
                            :record-id (:id record)
                            :reason :source-mismatch}))
        (if (= :accepted (:semantic-status record))
          (add-diagnostic! diagnostics
                           {:producer (:producer-id stream)
                            :record-id (:id record)
                            :reason :invalid-mirrored-event})
          (when-not (= :replay (:semantic-status record))
            (doseq [reason (:diagnostics record)]
              (add-diagnostic! diagnostics
                               {:producer (:producer-id stream)
                                :record-id (:id record)
                                :reason reason}))))))
    {:observations
     (mapv (fn [key]
             (assoc (get @observations key)
                    :dropped? (contains? @dropped key)))
           @observation-order)
     :reflections (mapv #(get @reflections %) @reflection-order)
     :diagnostics @diagnostics}))

(defn load-session-projection!
  [{:keys [store user-id session-id source-kind source-session-id
           embedded-projections]}]
  (-> (if (satisfies? store/AggregateMemoryStore store)
        (store/read-session-memory-streams! store user-id session-id)
        (js/Promise.resolve []))
      (.then
       (fn [streams]
         (aggregate-projection
          {:user-id user-id
           :source-kind source-kind
           :source-session-id source-session-id
           :embedded-projections embedded-projections
           :streams streams})))))
