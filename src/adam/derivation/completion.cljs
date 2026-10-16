(ns adam.derivation.completion
  (:require [adam.derivation.core :as core]
            [cljs.spec.alpha :as s]
            [clojure.string :as string]))

;; Shared by the two Pi command adapters. Completion is advisory and entirely
;; local: no configuration, graph, source corpus or model work on Tab.
(def ^:private common-options
  {"--depth" "Evidence depth" "--repository" "Git origin filter"
   "--session" "Source-scoped session URN filter" "--language" "Linked-file language filter"})
(def ^:private read-options
  {"--history" "Include saved history" "--limit" "Select 1–20 runs"
   "--evidence" "Inspect a saved conclusion ID" "--cursor" "Continue a read page"})
(def ^:private selectors #{"--repository" "--session" "--language"})
(def ^:private values
  {"--depth" (sort core/depths)
   "--language" (sort (keys core/language-extensions))
   "--limit" (map str (range 1 21))})
(s/def ::prefix (s/and string? #(<= (count %) 8192)))

(defn- valid-value? [flag value]
  (case flag
    "--depth" (contains? core/depths value)
    "--language" (s/valid? ::core/language value)
    "--limit" (and (re-matches #"[0-9]+" value) (<= 1 (js/Number value) 20))
    ;; Do not invent or look up free-form locators.
    (not (string/starts-with? value "--"))))

(defn- preceding-state [tokens options]
  (loop [remaining (if (and (seq tokens) (not (string/starts-with? (first tokens) "--"))) (rest tokens) tokens)
         used #{}]
    (if (empty? remaining)
      {:used used}
      (let [[flag value & more] remaining]
        (when (and (contains? options flag) (not (contains? used flag))
                   (not (and (selectors flag) (some selectors used))))
          (cond
            (= flag "--history") (recur (rest remaining) (conj used flag))
            (nil? value) {:used used :pending flag}
            (valid-value? flag value) (recur more (conj used flag))))))))

(defn arguments [read? prefix]
  (when (s/valid? ::prefix prefix)
    (let [options (cond-> common-options read? (merge read-options))
          current (or (re-find #"\S+$" prefix) "")
          before (subs prefix 0 (- (count prefix) (count current)))
          state (preceding-state (re-seq #"\S+" before) options)
          candidates (when state
                       (if-let [pending (:pending state)]
                         (get values pending)
                         (remove #(or (contains? (:used state) %)
                                      (and (selectors %) (some selectors (:used state)))) (sort (keys options)))))
          matches (filter #(string/starts-with? % current) candidates)]
      (when (seq matches)
        (clj->js (mapv (fn [candidate]
                        {:value (str before candidate) :label candidate
                         :description (get options (or (:pending state) candidate))}) matches))))))
