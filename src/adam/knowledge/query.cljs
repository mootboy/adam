(ns adam.knowledge.query
  (:require [adam.knowledge.evidence :as evidence]
            [adam.knowledge.repository :as repository]
            [adam.knowledge.store :as store]
            [adam.replica.identity :as identity]
            [clojure.string :as string]))

(def ^:private content-limit 500)

(defn query-error [code message]
  (ex-info message {:type :file-memory-query :code code}))

(defn query-error-code [error]
  (when (= :file-memory-query (:type (ex-data error)))
    (:code (ex-data error))))

(defn query-file-memory!
  [{:keys [get-store! get-user! resolve-repository!]} {:keys [cwd path limit]}]
  (let [path (if (string? path) (string/trim path) "")]
    (if (string/blank? path)
      (js/Promise.reject (query-error :missing-path "path must not be empty"))
      (-> (get-user!)
          (.then
           (fn [user]
             (let [user-uuid (:user-uuid user)
                   resolve! #(resolve-repository! % user-uuid)]
               (-> (resolve! cwd)
                   (.then (fn [resolved]
                            (if resolved
                              resolved
                              (repository/resolve-from-file-path! resolve! cwd path))))
                   (.then
                    (fn [resolved]
                      (when-not resolved
                        (throw (query-error :repository-not-found
                                            "path is not in a Git repository")))
                      (let [file (evidence/resolve-repository-file resolved cwd path)]
                        (when-not file
                          (throw (query-error
                                  :path-outside-repository
                                  "path must identify a file inside the resolved Git repository")))
                        (-> (get-store!)
                            (.then
                             (fn [memory-store]
                               (when-not (satisfies? store/FileMemoryQueryStore memory-store)
                                 (throw (js/Error. "file-memory queries are unavailable")))
                               (-> (store/query-file-memory!
                                    memory-store
                                    (identity/user-urn user-uuid)
                                    (:id resolved)
                                    (:relative-path file)
                                    limit)
                                   (.then
                                    (fn [memories]
                                      {:repository resolved
                                       :relative-path (:relative-path file)
                                       :memories memories})))))))))))))))))

(defn- compact-content [content]
  (let [normalized (string/trim (string/replace (or content "") #"\s+" " "))]
    (if (<= (count normalized) content-limit)
      normalized
      (str (subs normalized 0 (dec content-limit)) "…"))))

(defn render-file-memory-results [title path results]
  (string/join
   "\n"
   (into
    [(str title ": " path " (" (count results) " result"
          (when-not (= 1 (count results)) "s") ")")]
    (mapcat
     (fn [result]
       (let [state (when (and (= :observation (:kind result)) (:dropped? result))
                     ", dropped")
             contexts (->> (:source-contexts result)
                           (map (fn [{:keys [branch commit dirty?]}]
                                  (str (or branch "detached") " @ "
                                       (subs commit 0 (min 7 (count commit)))
                                       (when dirty? " (dirty)"))))
                           distinct
                           vec)]
         (cond->
          [(str "[" (name (:kind result)) " " (:memory-id result) state "] "
                (compact-content (:content result)))
           (str "  Session: " (:pi-session-id result) "; sources: "
                (if (seq (:source-entry-ids result))
                  (string/join ", " (:source-entry-ids result))
                  "none"))]
           (seq contexts) (conj (str "  Observed: " (string/join ", " contexts))))))
     results))))
