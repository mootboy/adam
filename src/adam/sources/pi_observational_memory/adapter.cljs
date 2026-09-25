(ns adam.sources.pi-observational-memory.adapter
  (:require [adam.sources.pi-observational-memory.schema :as schema]))

(def producer "pi-observational-memory")
(def adapter-version 1)

(defprotocol MemorySourceAdapter
  (extract-memories [adapter stored-entries]))

(defn- parse-entry [stored]
  (try
    (let [value (js/JSON.parse (:raw-json stored))]
      (when (and (schema/record-object? value)
                 (= (:entry-id stored) (aget value "id")))
        value))
    (catch :default _ nil)))

(defn- diagnostic [stored reason]
  {:producer producer
   :entry-id (:entry-id stored)
   :reason reason})

(defn- observation-map [recording-entry-id value]
  {:producer producer
   :adapter-version adapter-version
   :memory-id (aget value "id")
   :content (aget value "content")
   :timestamp (aget value "timestamp")
   :relevance (aget value "relevance")
   :token-count (aget value "tokenCount")
   :recording-entry-id recording-entry-id
   :source-entry-ids (vec (array-seq (aget value "sourceEntryIds")))})

(defn- reflection-map [recording-entry-id value]
  {:producer producer
   :adapter-version adapter-version
   :memory-id (aget value "id")
   :content (aget value "content")
   :token-count (aget value "tokenCount")
   :recording-entry-id recording-entry-id
   :supporting-observation-ids
   (vec (array-seq (aget value "supportingObservationIds")))})

(defrecord PiObservationalMemoryAdapter []
  MemorySourceAdapter
  (extract-memories [_ stored-entries]
    (let [observations (atom {})
          observation-order (atom [])
          reflections (atom {})
          reflection-order (atom [])
          dropped (atom #{})
          diagnostics (atom [])]
      (doseq [stored stored-entries]
        (when-let [value (parse-entry stored)]
          (let [custom-type (aget value "customType")]
            (when (and (= "custom" (aget value "type"))
                       (contains? schema/supported-custom-types custom-type))
              (let [data (aget value "data")]
                (cond
                  (not (schema/record-object? data))
                  (swap! diagnostics conj (diagnostic stored :malformed-entry))

                  (not= schema/supported-schema-version (schema/schema-version data))
                  (swap! diagnostics conj
                         (diagnostic stored :unsupported-schema-version))

                  (not (schema/valid-data? custom-type data))
                  (swap! diagnostics conj (diagnostic stored :malformed-entry))

                  (= custom-type schema/observations-recorded)
                  (doseq [item (array-seq (aget data "observations"))]
                    (let [memory-id (aget item "id")]
                      (when-not (contains? @observations memory-id)
                        (swap! observation-order conj memory-id)
                        (swap! observations assoc memory-id
                               (observation-map (:entry-id stored) item)))))

                  (= custom-type schema/reflections-recorded)
                  (doseq [item (array-seq (aget data "reflections"))]
                    (let [memory-id (aget item "id")]
                      (when-not (contains? @reflections memory-id)
                        (swap! reflection-order conj memory-id)
                        (swap! reflections assoc memory-id
                               (reflection-map (:entry-id stored) item)))))

                  (= custom-type schema/observations-dropped)
                  (swap! dropped into (array-seq (aget data "observationIds")))))))))
      {:producer producer
       :adapter-version adapter-version
       :observations (mapv (fn [memory-id]
                             (assoc (get @observations memory-id)
                                    :dropped? (contains? @dropped memory-id)))
                           @observation-order)
       :reflections (mapv #(get @reflections %) @reflection-order)
       :diagnostics @diagnostics})))

(def adapter (->PiObservationalMemoryAdapter))
