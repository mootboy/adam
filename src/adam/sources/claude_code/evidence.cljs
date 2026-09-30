(ns adam.sources.claude-code.evidence
  (:require [adam.knowledge.evidence :as common-evidence]
            [adam.replica.identity :as identity]
            [adam.sources.claude-code.scanner :as scanner]
            [clojure.string :as string]))

(def adapter-version 1)
(def ^:private supported-file-tools #{"Read" "Edit" "Write"})

(defn- json-object? [value]
  (and (some? value)
       (= "object" (goog/typeOf value))
       (not (js/Array.isArray value))))

(defn- message-blocks [value]
  (let [message (aget value "message")
        content (when (json-object? message) (aget message "content"))]
    (if (js/Array.isArray content) (array-seq content) [])))

(defn- file-tool-call [entry block]
  (when (and (json-object? block)
             (= "tool_use" (aget block "type"))
             (string? (aget block "id"))
             (contains? supported-file-tools (aget block "name")))
    (let [input (aget block "input")
          path (when (json-object? input) (aget input "file_path"))]
      (when (and (string? path) (not (string/blank? path)))
        {:call-id (aget block "id")
         :assistant-id (:record-uuid entry)
         :path path}))))

(defn explicit-file-locators [scan]
  (->> (scanner/selected-entries scan)
       (mapcat
        (fn [entry]
          (let [value (js/JSON.parse (:raw-json entry))]
            (keep (fn [block]
                    (when-let [{:keys [path]} (file-tool-call entry block)]
                      (when (:cwd entry)
                        {:cwd (:cwd entry) :path path})))
                  (message-blocks value)))))
       distinct
       vec))

(defn- tool-result-id [block]
  (when (and (json-object? block)
             (= "tool_result" (aget block "type"))
             (string? (aget block "tool_use_id")))
    (aget block "tool_use_id")))

(defn extract-projection [{:keys [user-uuid repository scan]}]
  (let [selected (scanner/selected-entries scan)
        files (atom {})
        calls (atom {})
        evidence (atom {})
        diagnostics (atom [])
        add-evidence!
        (fn [entry resolved]
          (let [{:keys [file worktree]} resolved
                item (cond-> {:entry-id (:entry-id entry)
                              :stream-id (:stream-id entry)
                              :file-id (:id file)
                              :commit (:commit worktree)
                              :dirty? (= true (:dirty? worktree))}
                       (:branch worktree) (assoc :branch (:branch worktree)))]
            (swap! files assoc (:id file) file)
            (swap! evidence assoc [(:stream-id entry) (:entry-id entry) (:id file)] item)))]
    (doseq [entry selected
            :let [value (js/JSON.parse (:raw-json entry))]
            block (message-blocks value)]
      (when-let [{:keys [call-id path] :as call} (file-tool-call entry block)]
        (when-let [resolved (and (:cwd entry)
                                 (common-evidence/resolve-repository-file-with-worktree
                                  repository (:cwd entry) path))]
          (swap! calls update call-id (fnil conj [])
                 (assoc call :entry entry :resolved resolved))
          (add-evidence! entry resolved))))
    (doseq [[call-id matching-calls] @calls
            :when (> (count matching-calls) 1)]
      (swap! diagnostics conj {:reason :duplicate-tool-use-id
                               :tool-use-id call-id
                               :entry-ids (mapv #(get-in % [:entry :entry-id]) matching-calls)}))
    (doseq [entry selected
            :let [value (js/JSON.parse (:raw-json entry))
                  source-assistant-id (aget value "sourceToolAssistantUUID")]
            block (message-blocks value)
            :let [result-id (tool-result-id block)]
            :when result-id]
      (let [matching (filterv #(= source-assistant-id (:assistant-id %))
                              (get @calls result-id []))]
        (when (= 1 (count matching))
          (add-evidence! entry (:resolved (first matching))))))
    (let [source-session-id (:source-session-id scan)]
      {:extractor-version common-evidence/extractor-version
       :adapter-version adapter-version
       :user-id (identity/user-urn user-uuid)
       :session-id (identity/session-urn user-uuid identity/claude-source-kind
                                         source-session-id)
       :source-kind identity/claude-source-kind
       :source-session-id source-session-id
       :repository repository
       :files (->> (vals @files) (sort-by :relative-path) vec)
       :entry-file-evidence (->> (vals @evidence)
                                 (sort-by (juxt :entry-id :stream-id :file-id))
                                 vec)
       :available-source-entries
       (set (map (juxt :stream-id :entry-id) (:entries scan)))
       :observations []
       :reflections []
       :memory-diagnostics (vec @diagnostics)})))
