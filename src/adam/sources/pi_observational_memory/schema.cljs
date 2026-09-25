(ns adam.sources.pi-observational-memory.schema
  (:require [clojure.string :as string]))

(def observations-recorded "om.observations.recorded")
(def reflections-recorded "om.reflections.recorded")
(def observations-dropped "om.observations.dropped")
(def supported-custom-types
  #{observations-recorded reflections-recorded observations-dropped})
(def supported-schema-version 1)

(def ^:private memory-id-pattern #"^[a-f0-9]{12}$")
(def ^:private relevance-values #{"low" "medium" "high" "critical"})

(defn record-object? [value]
  (and (some? value)
       (= "object" (goog/typeOf value))
       (not (js/Array.isArray value))))

(defn non-empty-string? [value]
  (and (string? value) (not (string/blank? value))))

(defn- non-empty-string-array? [value]
  (and (js/Array.isArray value)
       (pos? (.-length value))
       (every? non-empty-string? (array-seq value))))

(defn- token-count? [value]
  (and (number? value) (js/Number.isFinite value) (not (neg? value))))

(defn observation? [value]
  (and (record-object? value)
       (boolean (re-matches memory-id-pattern (or (aget value "id") "")))
       (non-empty-string? (aget value "content"))
       (non-empty-string? (aget value "timestamp"))
       (contains? relevance-values (aget value "relevance"))
       (non-empty-string-array? (aget value "sourceEntryIds"))
       (token-count? (aget value "tokenCount"))))

(defn reflection? [value]
  (let [content (when (record-object? value) (aget value "content"))]
    (and (record-object? value)
         (boolean (re-matches memory-id-pattern (or (aget value "id") "")))
         (non-empty-string? content)
         (not (re-find #"\r|\n" content))
         (non-empty-string-array? (aget value "supportingObservationIds"))
         (token-count? (aget value "tokenCount")))))

(defn schema-version [data]
  (when (record-object? data)
    (let [schema-version (aget data "schemaVersion")
          version (aget data "version")]
      (cond
        (some? schema-version) schema-version
        (some? version) version
        :else supported-schema-version))))

(defn valid-data? [custom-type data]
  (and (record-object? data)
       (= supported-schema-version (schema-version data))
       (non-empty-string? (aget data "coversUpToId"))
       (case custom-type
         "om.observations.recorded"
         (let [items (aget data "observations")]
           (and (js/Array.isArray items)
                (pos? (.-length items))
                (every? observation? (array-seq items))))

         "om.reflections.recorded"
         (let [items (aget data "reflections")]
           (and (js/Array.isArray items)
                (pos? (.-length items))
                (every? reflection? (array-seq items))))

         "om.observations.dropped"
         (non-empty-string-array? (aget data "observationIds"))

         false)))
