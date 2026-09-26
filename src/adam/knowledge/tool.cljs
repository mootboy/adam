(ns adam.knowledge.tool
  (:require [adam.knowledge.query :as query]
            [clojure.string :as string]))

(def result-limit 10)
(def query-limit (inc result-limit))
(def output-max-lines 200)
(def output-max-bytes 12000)

(def ^:private output-content-lines (- output-max-lines 2))
(def ^:private output-content-bytes 11400)

(def description
  (str "Retrieve observational memories deterministically linked to a repository file. "
       "Use this before changing a file when prior decisions or rationale may matter. "
       "Accepts local paths, or an optional Git origin with a repository-relative path; "
       "it is not semantic search."))

(defn- prefix-within-bytes [text max-bytes]
  (loop [low 0 high (count text)]
    (if (>= low high)
      (let [end low
            end (if (and (pos? end)
                         (<= 0xD800 (.charCodeAt text (dec end)) 0xDBFF))
                  (dec end)
                  end)]
        (subs text 0 end))
      (let [middle (.ceil js/Math (/ (+ low high) 2))]
        (if (<= (.byteLength js/Buffer (subs text 0 middle) "utf8") max-bytes)
          (recur middle high)
          (recur low (dec middle)))))))

(defn truncate-output [text]
  (let [all-lines (string/split text #"\n" -1)
        line-truncated? (> (count all-lines) output-content-lines)
        line-content (string/join "\n" (take output-content-lines all-lines))
        byte-truncated? (> (.byteLength js/Buffer line-content "utf8") output-content-bytes)
        content (if byte-truncated?
                  (prefix-within-bytes line-content output-content-bytes)
                  line-content)
        truncated? (or line-truncated? byte-truncated?)]
    (if-not truncated?
      {:content text :truncated? false}
      (let [notice (str "[Output truncated: showing at most " output-content-lines
                        " of " (count all-lines) " lines and "
                        (.byteLength js/Buffer content "utf8") " of "
                        (.byteLength js/Buffer text "utf8") " bytes.]")]
        {:content (str content "\n\n" notice)
         :truncated? true}))))

(defn execute!
  [dependencies {:keys [cwd path origin] :as parameters}]
  (let [request (cond-> {:cwd cwd :path path :limit query-limit}
                  (contains? parameters :origin) (assoc :origin origin))]
    (-> (query/query-file-memory! dependencies request)
        (.then
         (fn [{:keys [repository relative-path memories]}]
           (let [shown (vec (take result-limit memories))
                 item-truncated? (> (count memories) (count shown))]
             (if (empty? memories)
               {:content (str "No code-linked memories found for " relative-path)
                :details {:status "empty"
                          :path relative-path
                          :repository-id (:id repository)
                          :result-count 0
                          :truncated? false}}
               (let [base (query/render-file-memory-results
                           "adam file context" relative-path shown)
                     rendered (if item-truncated?
                                (str base "\n\n[" result-limit
                                     " file memories shown; additional results omitted.]")
                                base)
                     truncated (truncate-output rendered)]
                 {:content (:content truncated)
                  :details {:status "ok"
                            :path relative-path
                            :repository-id (:id repository)
                            :result-count (count shown)
                            :truncated? (or item-truncated? (:truncated? truncated))}}))))))))
