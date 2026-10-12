(ns adam.derivation.config
  (:require [cljs.spec.alpha :as s]
            [clojure.string :as string]
            ["yaml" :as yaml]
            ["node:buffer" :refer [Buffer]]
            ["node:path" :as path]))

(defn reject! [code message]
  (throw (ex-info message {:type :derivation :code code})))

(defn error-code [error]
  (when (= :derivation (:type (ex-data error))) (:code (ex-data error))))

;; Shared scalar specs; callers compose them into their local domain contracts.
(defn text-spec [maximum]
  (s/and string? #(not (string/blank? %))
         #(<= (.byteLength Buffer % "utf8") maximum)
         #(not (re-find #"[\u0000-\u0008\u000b\u000c\u000e-\u001f\u007f]" %))))

(defn only-keys [allowed]
  (fn [value] (every? allowed (keys value))))

(s/def ::positive-integer (s/and number? js/Number.isSafeInteger pos?))
(s/def ::name (s/and string? #(re-matches #"[a-z][a-z0-9_-]{0,63}" %)))
(s/def ::yaml-name (s/and keyword? #(nil? (namespace %)) #(s/valid? ::name (name %))))
(s/def ::version ::positive-integer)
(s/def ::guidance (text-spec 65536))
(s/def ::provider (text-spec 256))
(s/def ::id (text-spec 512))
(s/def ::model (s/and (s/keys :req-un [::provider ::id]) (only-keys #{:provider :id})))
(s/def ::max_input_tokens ::positive-integer)
(s/def ::max_output_tokens ::positive-integer)
(s/def ::timeout_ms (s/and ::positive-integer #(<= % 2147483647)))
(s/def ::limits
  (s/and (s/keys :opt-un [::max_input_tokens ::max_output_tokens ::timeout_ms])
         (only-keys #{:max_input_tokens :max_output_tokens :timeout_ms})))
(s/def ::category
  (s/and (s/keys :req-un [::guidance]) (only-keys #{:guidance})
         #(s/valid? (text-spec 8192) (:guidance %))))
(s/def ::categories (s/map-of ::yaml-name ::category :max-count 64))
(s/def ::require_supporting_memory_ids #{true})
(s/def ::inferred_min_distinct_occasions ::positive-integer)
(s/def ::evidence
  (s/and (s/keys :opt-un [::require_supporting_memory_ids ::inferred_min_distinct_occasions])
         (only-keys #{:require_supporting_memory_ids :inferred_min_distinct_occasions})))
(s/def ::distinguish
  (s/coll-of #{"personal" "repository" "group" "session"} :kind vector? :min-count 1 :distinct true))
(s/def ::scope (s/and (s/keys :req-un [::distinguish]) (only-keys #{:distinguish})))
(s/def ::policy #{"retain_and_highlight"})
(s/def ::conflicts (s/and (s/keys :req-un [::policy]) (only-keys #{:policy})))
(s/def ::recipe
  (s/and (s/keys :req-un [::version ::guidance]
                 :opt-un [::model ::limits ::categories ::evidence ::scope ::conflicts])
         (only-keys #{:version :guidance :model :limits :categories :evidence :scope :conflicts})))
(s/def ::derivations (s/map-of ::yaml-name ::recipe :min-count 1 :max-count 64))
(s/def ::configuration (s/and (s/keys :req-un [::derivations]) (only-keys #{:derivations})))
(s/def ::source (s/and string? #(<= (.byteLength Buffer % "utf8") 262144)))
(s/def ::config-home (s/and (text-spec 4096) #(.isAbsolute path %)))

(defn config-path [config-home]
  (when-not (s/valid? ::config-home config-home)
    (reject! :invalid-config-home "configuration root must be absolute"))
  (.join path config-home "adam" "derivations.yaml"))

(defn- compile-recipe [{:keys [version guidance model limits categories evidence scope]}]
  (cond-> {:version version :guidance guidance
           :limits (cond-> {:max-output-tokens (get limits :max_output_tokens 8192)
                            :timeout-ms (get limits :timeout_ms 300000)}
                     (contains? limits :max_input_tokens)
                     (assoc :max-input-tokens (:max_input_tokens limits)))
           :categories (into (sorted-map) (map (fn [[key value]] [(name key) (:guidance value)]) categories))
           :inferred-min-distinct-occasions (get evidence :inferred_min_distinct_occasions 2)
           :scope-types (set (get scope :distinguish ["personal" "repository" "group" "session"]))}
    model (assoc :model model)))

(defn parse-config [source]
  (when-not (s/valid? ::source source)
    (reject! :invalid-config "configuration exceeds 256 KiB"))
  (let [value (try
                (let [document (.parseDocument yaml source #js {:version "1.2" :uniqueKeys true :merge false})]
                  (when (or (seq (.-errors document)) (seq (.-warnings document)))
                    (reject! :invalid-config "invalid YAML"))
                  (js->clj (.toJS document #js {:maxAliasCount 0}) :keywordize-keys true))
                (catch :default _ (reject! :invalid-config "invalid YAML")))]
    ;; Do not attach explain-data: it would retain untrusted values/secrets.
    (when-not (s/valid? ::configuration value)
      (reject! :invalid-config "invalid derivation configuration"))
    (into (sorted-map) (map (fn [[key recipe]] [(name key) (compile-recipe recipe)]) (:derivations value)))))
