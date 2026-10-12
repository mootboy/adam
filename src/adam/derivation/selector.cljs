(ns adam.derivation.selector
  (:require [adam.derivation.config :as config]
            [adam.knowledge.evidence :as evidence]
            [clojure.string :as string]))

(def language-extensions
  {"python" #{"py" "pyi"} "javascript" #{"js" "jsx" "mjs" "cjs"}
   "typescript" #{"ts" "tsx" "mts" "cts"} "clojure" #{"clj" "cljc"}
   "clojurescript" #{"cljs" "cljc"} "edn" #{"edn"}
   "java" #{"java"} "kotlin" #{"kt" "kts"} "rust" #{"rs"} "go" #{"go"}
   "ruby" #{"rb"} "c" #{"c" "h"} "cpp" #{"cpp" "cc" "cxx" "hpp" "h"}
   "shell" #{"sh" "bash" "zsh"} "sql" #{"sql"} "html" #{"html" "htm"}
   "css" #{"css"} "json" #{"json"} "yaml" #{"yaml" "yml"}
   "markdown" #{"md" "markdown"}})

(def ^:private aliases
  {"py" "python" "js" "javascript" "ts" "typescript"
   "clj" "clojure" "cljs" "clojurescript" "c++" "cpp"
   "sh" "shell" "yml" "yaml" "md" "markdown"})

(defn normalize-language [value]
  (when-not (config/text? value 64)
    (config/reject! :invalid-selector "language must be a supported name"))
  (let [lower (string/lower-case (string/trim value))
        language (get aliases lower lower)]
    (when-not (contains? language-extensions language)
      (config/reject! :invalid-selector "language must be a supported name"))
    language))

(defn normalize-refresh [request]
  (when-not (and (map? request)
                 (every? #{:name :repository :session :language} (keys request)))
    (config/reject! :invalid-selector "unknown refresh arguments"))
  (when (contains? request :name)
    (when-not (and (string? (:name request))
                   (re-matches #"[a-z][a-z0-9_-]{0,63}" (:name request)))
      (config/reject! :invalid-selector "invalid derivation name")))
  (let [filters (filter #(contains? request %) [:repository :session :language])]
    (when (> (count filters) 1)
      (config/reject! :invalid-selector "use at most one repository, session or language filter"))
    (let [kind (first filters)
          supplied (get request kind)
          value (case kind
                  :repository
                  (do
                    (when-not (config/text? supplied 4096)
                      (config/reject! :invalid-selector "invalid Git origin"))
                    (or (evidence/normalize-git-remote supplied)
                        (config/reject! :invalid-selector "invalid Git origin")))
                  :session
                  (do
                    (when-not (and (config/text? supplied 2048)
                                   (re-matches #"urn:adam:session:[^:\s]+:(?:pi|claude-code):[^\s]+" supplied))
                      (config/reject! :invalid-selector "session must be a canonical source-scoped identity"))
                    supplied)
                  :language (normalize-language supplied)
                  nil)]
      (cond-> {:filter (if kind {:type kind :value value} {:type :none})}
        (:name request) (assoc :name (:name request))))))

(defn normalize-read [request]
  (when-not (and (map? request)
                 (every? #{:name :repository :session :language :history :limit} (keys request)))
    (config/reject! :invalid-selector "unknown read arguments"))
  (when (and (contains? request :history) (not (boolean? (:history request))))
    (config/reject! :invalid-selector "history must be boolean"))
  (let [limit (get request :limit 10)]
    (when-not (and (config/positive-integer? limit) (<= limit 20))
      (config/reject! :invalid-selector "read limit must be between 1 and 20"))
    (cond-> (assoc (normalize-refresh (dissoc request :history :limit))
                   :history? (get request :history false) :limit limit)
      (not-any? #(contains? request %) [:repository :session :language])
      (assoc :filter nil))))

(defn matches-filter? [{:keys [type value]} memory]
  (case type
    :none true
    :session (= value (:session-id memory))
    :repository (contains? (set (:repository-origins memory)) value)
    :language (boolean
                (some #(let [extension (last (string/split (string/lower-case %) #"\."))]
                         (contains? (get language-extensions value) extension))
                      (:linked-paths memory)))
    false))
