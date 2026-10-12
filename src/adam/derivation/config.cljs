(ns adam.derivation.config
  (:require [clojure.string :as string]
            ["yaml" :as yaml]
            ["node:buffer" :refer [Buffer]]
            ["node:path" :as path]))

(defn reject! [code message]
  (throw (ex-info message {:type :derivation :code code})))

(defn error-code [error]
  (when (= :derivation (:type (ex-data error))) (:code (ex-data error))))

(defn text? [value maximum]
  (and (string? value) (not (string/blank? value))
       (<= (.byteLength Buffer value "utf8") maximum)
       (not (re-find #"[\u0000-\u0008\u000b\u000c\u000e-\u001f\u007f]" value))))

(defn positive-integer? [value]
  (and (number? value) (js/Number.isSafeInteger value) (pos? value)))

(defn fields! [value allowed]
  (when-not (and (map? value) (every? allowed (keys value)))
    (reject! :invalid-config "invalid or unknown configuration fields")))

(defn config-path [config-home]
  (when-not (and (text? config-home 4096) (.isAbsolute path config-home))
    (reject! :invalid-config-home "configuration root must be absolute"))
  (.join path config-home "adam" "derivations.yaml"))

(defn- compile-recipe [recipe]
  (fields! recipe #{"version" "guidance" "model" "limits" "categories" "evidence" "scope" "conflicts"})
  (when-not (and (positive-integer? (get recipe "version"))
                (text? (get recipe "guidance") 65536))
    (reject! :invalid-config "version and guidance are required"))
  (let [model (get recipe "model")
        limits (get recipe "limits" {})
        categories (get recipe "categories" {})
        evidence (get recipe "evidence" {})
        scope (get recipe "scope" {"distinguish" ["personal" "repository" "group" "session"]})
        conflicts (get recipe "conflicts" {"policy" "retain_and_highlight"})]
    (when (contains? recipe "model")
      (fields! model #{"provider" "id"})
      (when-not (and (text? (get model "provider") 256) (text? (get model "id") 512))
        (reject! :invalid-config "model requires provider and id")))
    (fields! limits #{"max_input_tokens" "max_output_tokens" "timeout_ms"})
    (when-not (and (every? positive-integer? (vals limits))
                   (<= (get limits "timeout_ms" 300000) 2147483647))
      (reject! :invalid-config "limits must be positive safe integers within the native timer range"))
    (fields! evidence #{"require_supporting_memory_ids" "inferred_min_distinct_occasions"})
    (when (or (and (contains? evidence "require_supporting_memory_ids")
                  (not= true (get evidence "require_supporting_memory_ids")))
              (and (contains? evidence "inferred_min_distinct_occasions")
                   (not (positive-integer? (get evidence "inferred_min_distinct_occasions")))))
      (reject! :invalid-config "support is mandatory and occasion threshold must be positive"))
    (fields! scope #{"distinguish"})
    (let [subjects (get scope "distinguish")]
      (when-not (and (vector? subjects) (seq subjects)
                     (= (count subjects) (count (set subjects)))
                     (every? #{"personal" "repository" "group" "session"} subjects))
        (reject! :invalid-config "invalid scope kinds")))
    (fields! conflicts #{"policy"})
    (when-not (= "retain_and_highlight" (get conflicts "policy"))
      (reject! :invalid-config "conflicts must be retained and highlighted"))
    (when-not (and (map? categories) (<= (count categories) 64))
      (reject! :invalid-config "invalid categories"))
    (doseq [[category instructions] categories]
      (when-not (and (string? category) (re-matches #"[a-z][a-z0-9_-]{0,63}" category))
        (reject! :invalid-config "invalid category name"))
      (fields! instructions #{"guidance"})
      (when-not (text? (get instructions "guidance") 8192)
        (reject! :invalid-config "category guidance is required")))
    (cond-> {:version (get recipe "version") :guidance (get recipe "guidance")
             :limits (cond-> {:max-output-tokens (get limits "max_output_tokens" 8192)
                              :timeout-ms (get limits "timeout_ms" 300000)}
                       (contains? limits "max_input_tokens")
                       (assoc :max-input-tokens (get limits "max_input_tokens")))
             :categories (into (sorted-map) (map (fn [[key value]] [key (get value "guidance")]) categories))
             :inferred-min-distinct-occasions (get evidence "inferred_min_distinct_occasions" 2)
             :scope-types (set (get scope "distinguish"))}
      model (assoc :model {:provider (get model "provider") :id (get model "id")}))))

(defn parse-config [source]
  (when-not (and (string? source) (<= (.byteLength Buffer source "utf8") 262144))
    (reject! :invalid-config "configuration exceeds 256 KiB"))
  (let [value
        (try
          (let [document (.parseDocument yaml source #js {:version "1.2" :uniqueKeys true :merge false})]
            (when (or (seq (.-errors document)) (seq (.-warnings document)))
              (reject! :invalid-config "invalid YAML"))
            (js->clj (.toJS document #js {:maxAliasCount 0})))
          (catch :default _ (reject! :invalid-config "invalid YAML")))]
    (fields! value #{"derivations"})
    (let [recipes (get value "derivations")]
      (when-not (and (map? recipes) (<= 1 (count recipes) 64))
        (reject! :invalid-config "configure between 1 and 64 named derivations"))
      (into (sorted-map)
            (map (fn [[name recipe]]
                   (when-not (and (string? name) (re-matches #"[a-z][a-z0-9_-]{0,63}" name))
                     (reject! :invalid-config "invalid derivation name"))
                   [name (compile-recipe recipe)]) recipes)))))
