(ns adam.memory.protocol
  (:require [clojure.string :as string]
            ["node:crypto" :refer [createHash]]))

(def supported-kinds
  #{"observations.recorded"
    "reflections.recorded"
    "observations.dropped"
    "source.covered"})

(defn json-object? [value]
  (and (some? value)
       (= "object" (goog/typeOf value))
       (not (js/Array.isArray value))))

(defn sha256
  ([value]
   (-> (createHash "sha256") (.update value) (.digest "hex")))
  ([left right]
   (-> (createHash "sha256") (.update left) (.update right) (.digest "hex"))))

(defn nonblank-string? [value]
  (and (string? value) (not (string/blank? value))))

(defn- compare-utf16 [left right]
  (cond (< left right) -1 (> left right) 1 :else 0))

(defn- valid-unicode-string? [value]
  (loop [index 0]
    (if (>= index (.-length value))
      true
      (let [unit (.charCodeAt value index)]
        (cond
          (<= 0xD800 unit 0xDBFF)
          (and (< (inc index) (.-length value))
               (let [next-unit (.charCodeAt value (inc index))]
                 (and (<= 0xDC00 next-unit 0xDFFF)
                      (recur (+ index 2)))))

          (<= 0xDC00 unit 0xDFFF) false
          :else (recur (inc index)))))))

(declare canonical-json)

(defn- canonical-object [value]
  (let [keys (sort compare-utf16 (array-seq (js/Object.keys value)))]
    (when-not (every? valid-unicode-string? keys)
      (throw (js/Error. "JCS rejects lone Unicode surrogates")))
    (str "{" (string/join "," (map #(str (js/JSON.stringify %) ":"
                                           (canonical-json (aget value %)))
                                      keys)) "}")))

(defn canonical-json [value]
  (cond
    (nil? value) "null"
    (string? value) (if (valid-unicode-string? value)
                      (js/JSON.stringify value)
                      (throw (js/Error. "JCS rejects lone Unicode surrogates")))
    (or (true? value) (false? value)) (js/JSON.stringify value)
    (number? value) (if (js/Number.isFinite value)
                      (js/JSON.stringify value)
                      (throw (js/Error. "JCS rejects non-finite numbers")))
    (js/Array.isArray value)
    (str "[" (string/join "," (map canonical-json (array-seq value))) "]")
    (json-object? value) (canonical-object value)
    :else (throw (js/Error. "unsupported JCS value"))))

(defn event-hash [value]
  (sha256 (canonical-json value)))

(declare host-id? integer-in-range? has-own?)

(defn- checkpoint-streams [value]
  (let [checkpoint (when (json-object? value) (aget value "sourceCheckpoint"))
        streams (when (json-object? checkpoint) (aget checkpoint "streams"))]
    (when (and (js/Array.isArray streams)
               (<= 1 (.-length streams) 256))
      (let [parsed
            (mapv (fn [stream]
                    (let [leaf (when (json-object? stream)
                                 (aget stream "selectedLeafEntryId"))]
                      (when (and (json-object? stream)
                                 (host-id? (aget stream "streamId"))
                                 (integer-in-range? (aget stream "committedBytes"))
                                 (re-matches #"[a-f0-9]{64}"
                                             (or (aget stream "prefixSha256") ""))
                                 (has-own? stream "selectedLeafEntryId")
                                 (or (nil? leaf) (host-id? leaf)))
                        {:stream-id (aget stream "streamId")
                         :committed-bytes (aget stream "committedBytes")
                         :prefix-sha256 (aget stream "prefixSha256")
                         :selected-leaf-entry-id leaf})))
                  (array-seq streams))]
        (when (and (seq parsed)
                   (every? some? parsed)
                   (= (mapv :stream-id parsed)
                      (->> parsed (map :stream-id) (sort compare-utf16) vec))
                   (= (count parsed) (count (set (map :stream-id parsed)))))
          parsed)))))

(defn checkpoint-regression? [previous incoming]
  (let [incoming-by-id (into {} (map (juxt :stream-id identity) incoming))]
    (boolean
     (some (fn [{:keys [stream-id committed-bytes prefix-sha256]}]
             (let [candidate (get incoming-by-id stream-id)]
               (or (nil? candidate)
                   (< (:committed-bytes candidate) committed-bytes)
                   (and (= (:committed-bytes candidate) committed-bytes)
                        (not= (:prefix-sha256 candidate) prefix-sha256)))))
           previous))))

(defn- has-own? [value key]
  (js/Object.hasOwn value key))

(defn- bounded-string? [value minimum maximum]
  (and (string? value) (<= minimum (.-length value) maximum)))

(defn- protocol-id? [value]
  (and (bounded-string? value 1 128)
       (boolean (re-matches #"[a-z0-9][a-z0-9._-]{0,127}" value))))

(defn- stable-id? [value]
  (and (bounded-string? value 1 256)
       (boolean (re-matches #"[A-Za-z0-9][A-Za-z0-9._-]{0,255}" value))))

(defn- host-id? [value]
  (and (bounded-string? value 1 512)
       (not (re-find #"[\u0000-\u001f\u007f]" value))))

(defn- memory-id? [value]
  (and (string? value) (boolean (re-matches #"[a-f0-9]{12}" value))))

(defn- timestamp? [value]
  (and (bounded-string? value 1 64)
       (string/ends-with? value "Z")
       (not (js/Number.isNaN (js/Date.parse value)))))

(defn- bounded-array? [value minimum maximum]
  (and (js/Array.isArray value)
       (<= minimum (.-length value) maximum)))

(defn- unique-by? [f values]
  (= (count values) (count (set (map f values)))))

(defn- integer-in-range? [value]
  (and (number? value) (js/Number.isSafeInteger value) (not (neg? value))))

(defn valid-locator? [{:keys [source-kind source-session-id producer-id]}]
  (and (protocol-id? source-kind)
       (host-id? source-session-id)
       (protocol-id? producer-id)))

(defn- source-entry? [value checkpoint-stream-ids]
  (and (json-object? value)
       (host-id? (aget value "streamId"))
       (host-id? (aget value "entryId"))
       (contains? checkpoint-stream-ids (aget value "streamId"))))

(defn- observation? [value checkpoint-stream-ids]
  (let [entries (when (json-object? value) (aget value "sourceEntries"))]
    (and (json-object? value)
         (memory-id? (aget value "id"))
         (bounded-string? (aget value "content") 1 65536)
         (timestamp? (aget value "timestamp"))
         (contains? #{"low" "medium" "high" "critical"} (aget value "relevance"))
         (bounded-array? entries 1 256)
         (every? #(source-entry? % checkpoint-stream-ids) (array-seq entries))
         (unique-by? #(str (aget % "streamId") "\u0000" (aget % "entryId"))
                     (array-seq entries))
         (integer-in-range? (aget value "tokenCount")))))

(defn- reflection? [value]
  (let [supports (when (json-object? value) (aget value "supportingObservationIds"))]
    (and (json-object? value)
         (memory-id? (aget value "id"))
         (bounded-string? (aget value "content") 1 65536)
         (not (re-find #"[\r\n]" (aget value "content")))
         (bounded-array? supports 1 256)
         (every? memory-id? (array-seq supports))
         (unique-by? identity (array-seq supports))
         (integer-in-range? (aget value "tokenCount")))))

(defn- supported-shape? [value source-checkpoint]
  (let [kind (aget value "kind")
        stream-ids (set (map :stream-id source-checkpoint))
        observations (aget value "observations")
        reflections (aget value "reflections")
        observation-ids (aget value "observationIds")
        no-observations? (not (has-own? value "observations"))
        no-reflections? (not (has-own? value "reflections"))
        no-drops? (not (has-own? value "observationIds"))]
    (and
     (stable-id? (aget value "eventId"))
     (timestamp? (aget value "recordedAt"))
     (case kind
       "observations.recorded"
       (and no-reflections? no-drops?
            (bounded-array? observations 1 256)
            (every? #(observation? % stream-ids) (array-seq observations))
            (unique-by? #(aget % "id") (array-seq observations)))

       "reflections.recorded"
       (and no-observations? no-drops?
            (bounded-array? reflections 1 256)
            (every? reflection? (array-seq reflections))
            (unique-by? #(aget % "id") (array-seq reflections)))

       "observations.dropped"
       (and no-observations? no-reflections?
            (bounded-array? observation-ids 1 256)
            (every? memory-id? (array-seq observation-ids))
            (unique-by? identity (array-seq observation-ids)))

       "source.covered" (and no-observations? no-reflections? no-drops?)
       false))))

(defn inspect-event [value locator]
  (let [producer (when (json-object? value) (aget value "producer"))
        source (when (json-object? value) (aget value "source"))
        source-checkpoint (checkpoint-streams value)]
    (cond
      (not (json-object? value))
      {:semantic-status :skipped :diagnostics [:invalid-envelope]}

      (not= 1 (aget value "protocolVersion"))
      {:semantic-status :skipped :diagnostics [:unsupported-version]}

      (not (contains? supported-kinds (aget value "kind")))
      {:semantic-status :skipped :diagnostics [:unsupported-kind]}

      (or (not (json-object? producer))
          (not (protocol-id? (aget producer "id")))
          (not (bounded-string? (aget producer "version") 1 128))
          (nil? source-checkpoint)
          (not (json-object? source))
          (not (protocol-id? (aget source "kind")))
          (not (host-id? (aget source "sessionId")))
          (not (supported-shape? value source-checkpoint)))
      {:semantic-status :skipped :diagnostics [:invalid-envelope]}

      (not= (:producer-id locator) (aget producer "id"))
      {:semantic-status :skipped :diagnostics [:producer-mismatch]}

      (or (not= (:source-kind locator) (aget source "kind"))
          (not= (:source-session-id locator) (aget source "sessionId")))
      {:semantic-status :skipped :diagnostics [:source-mismatch]}

      :else
      {:semantic-status :accepted
       :diagnostics []
       :event-id (aget value "eventId")
       :event-hash (event-hash value)
       :kind (aget value "kind")
       :source-checkpoint source-checkpoint})))
