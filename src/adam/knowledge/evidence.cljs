(ns adam.knowledge.evidence
  (:require [adam.replica.identity :as identity]
            [clojure.string :as string]
            ["node:crypto" :refer [createHash]]
            ["node:path" :as node-path]))

(def extractor-version 2)
(def ^:private supported-file-tools #{"read" "edit" "write"})

(defn- sha256 [value]
  (-> (createHash "sha256") (.update value "utf8") (.digest "hex")))

(defn normalize-git-remote [remote]
  (let [trimmed (some-> remote string/trim)]
    (when-not (string/blank? trimmed)
      (if-let [[_ host path] (and (not (string/includes? trimmed "://"))
                                  (re-matches #"^(?:[^@/\s]+@)?([^:/\s]+):(.+)$" trimmed))]
        (str (string/lower-case host) "/"
             (-> path
                 (string/replace #"^/+|/+$" "")
                 (string/replace #"(?i)\.git$" "")))
        (try
          (let [url (js/URL. trimmed)
                host (string/lower-case (.-hostname url))
                path (-> (.-pathname url)
                         (string/replace #"^/+|/+$" "")
                         (string/replace #"(?i)\.git$" ""))]
            (when (and (not (string/blank? host))
                       (not (string/blank? path)))
              (str host "/" path)))
          (catch :default _ nil))))))

(defn build-repository
  [{:keys [user-uuid root remote commit branch dirty? worktrees]}]
  (let [root (.resolve node-path root)
        normalized-remote (normalize-git-remote remote)
        repository-hash (sha256 (or normalized-remote root))
        normalize-worktree
        (fn [worktree]
          (cond-> (assoc worktree :root (.resolve node-path (:root worktree)))
            (string/blank? (:branch worktree)) (dissoc :branch)))
        worktrees (mapv normalize-worktree
                        (or worktrees
                            [{:root root :commit commit :branch branch :dirty? dirty?}]))]
    (cond-> {:id (str "urn:adam:repository:" user-uuid ":" repository-hash)
             :user-id (identity/user-urn user-uuid)
             :root root
             :commit commit
             :dirty? (= true dirty?)
             :worktrees worktrees}
      normalized-remote (assoc :normalized-remote normalized-remote)
      (not (string/blank? branch)) (assoc :branch (string/trim branch)))))

(defn- inside-relative-path? [relative-path]
  (and (not (string/blank? relative-path))
       (not= ".." relative-path)
       (not (string/starts-with? relative-path (str ".." (.-sep node-path))))
       (not (.isAbsolute node-path relative-path))))

(defn resolve-repository-file-with-worktree [repository cwd path]
  (when-not (string/blank? path)
    (let [absolute (.resolve node-path (if (.isAbsolute node-path path)
                                        path
                                        (.resolve node-path cwd path)))
          match (->> (:worktrees repository)
                     (keep (fn [worktree]
                             (let [relative-path (.relative node-path (:root worktree) absolute)]
                               (when (inside-relative-path? relative-path)
                                 {:worktree worktree :relative-path relative-path}))))
                     (sort-by #(count (get-in % [:worktree :root])) >)
                     first)]
      (when match
        (let [relative-path (string/replace (:relative-path match) (.-sep node-path) "/")
              file-hash (sha256 (str (:id repository) "\u0000" relative-path))]
          {:file {:id (str "urn:adam:file:" file-hash)
                  :repository-id (:id repository)
                  :relative-path relative-path}
           :worktree (:worktree match)})))))

(defn resolve-repository-file [repository cwd path]
  (:file (resolve-repository-file-with-worktree repository cwd path)))

(defn- map-object? [value]
  (and (some? value)
       (= "object" (goog/typeOf value))
       (not (js/Array.isArray value))))

(defn- parsed-entries [entries]
  (keep (fn [stored]
          (try
            (let [value (js/JSON.parse (:raw-json stored))]
              (when (and (map-object? value)
                         (= (:entry-id stored) (aget value "id")))
                {:stored stored :value value}))
            (catch :default _ nil)))
        entries))

(defn- selected-entries [entries current-leaf-id]
  (let [parsed (vec (parsed-entries entries))
        selected-leaf-id (if (= ::unspecified current-leaf-id)
                           (:entry-id (last entries))
                           current-leaf-id)
        by-id (into {} (map (fn [item] [(get-in item [:stored :entry-id]) item]) parsed))
        active-ids
        (loop [cursor selected-leaf-id
               active #{}]
          (if (or (nil? cursor) (contains? active cursor))
            active
            (if-let [item (get by-id cursor)]
              (let [parent-id (aget (:value item) "parentId")]
                (recur (when (string? parent-id) parent-id) (conj active cursor)))
              active)))]
    (filterv #(contains? active-ids (get-in % [:stored :entry-id])) parsed)))

(defn selected-stored-entries
  ([entries] (selected-stored-entries entries ::unspecified))
  ([entries current-leaf-id]
   (mapv :stored (selected-entries entries current-leaf-id))))

(defn- tool-call-path [block]
  (when (and (map-object? block)
             (= "toolCall" (aget block "type"))
             (string? (aget block "id"))
             (contains? supported-file-tools (aget block "name")))
    (let [arguments (aget block "arguments")
          path (when (map-object? arguments) (aget arguments "path"))]
      (when (and (string? path) (not (string/blank? path)))
        {:call-id (aget block "id") :path path}))))

(defn explicit-file-tool-paths
  ([entries] (explicit-file-tool-paths entries ::unspecified))
  ([entries current-leaf-id]
   (->> (selected-entries entries current-leaf-id)
        (mapcat
         (fn [{:keys [value]}]
           (let [message (aget value "message")]
             (if (and (map-object? message)
                      (= "assistant" (aget message "role"))
                      (js/Array.isArray (aget message "content")))
               (keep (fn [block] (:path (tool-call-path block)))
                     (array-seq (aget message "content")))
               []))))
        distinct
        vec)))

(defn extract-projection
  [{:keys [user-uuid pi-session-id session-id cwd repository entries memory-projection]
    :as input}]
  (let [selected (selected-entries entries
                                   (if (contains? input :current-leaf-id)
                                     (:current-leaf-id input)
                                     ::unspecified))
        files (atom {})
        paths-by-call (atom {})
        evidence-by-entry (atom {})
        add-evidence!
        (fn [entry-id resolved]
          (let [{:keys [file worktree]} resolved
                evidence (cond-> {:entry-id entry-id
                                  :file-id (:id file)
                                  :commit (:commit worktree)
                                  :dirty? (= true (:dirty? worktree))}
                           (:branch worktree) (assoc :branch (:branch worktree)))]
            (swap! files assoc (:id file) file)
            (swap! evidence-by-entry assoc [entry-id (:id file)] evidence)))]
    (doseq [{:keys [stored value]} selected]
      (let [message (aget value "message")]
        (when (and (map-object? message)
                   (= "assistant" (aget message "role"))
                   (js/Array.isArray (aget message "content")))
          (doseq [block (array-seq (aget message "content"))]
            (when-let [{:keys [call-id path]} (tool-call-path block)]
              (when-let [resolved (resolve-repository-file-with-worktree repository cwd path)]
                (swap! paths-by-call assoc call-id resolved)
                (add-evidence! (:entry-id stored) resolved)))))))
    (doseq [{:keys [stored value]} selected]
      (let [message (aget value "message")]
        (when (and (map-object? message)
                   (= "toolResult" (aget message "role"))
                   (string? (aget message "toolCallId")))
          (when-let [resolved (get @paths-by-call (aget message "toolCallId"))]
            (add-evidence! (:entry-id stored) resolved)))))
    (let [project-observation
          (fn [memory]
            (let [file-ids
                  (->> (:source-entry-ids memory)
                       (mapcat (fn [entry-id]
                                 (keep (fn [[[evidence-entry-id file-id] _]]
                                         (when (= entry-id evidence-entry-id) file-id))
                                       @evidence-by-entry)))
                       distinct
                       sort
                       vec)]
              (assoc memory
                     :id (str "urn:adam:observation:" user-uuid ":" pi-session-id
                              ":" (:memory-id memory))
                     :file-ids file-ids)))
          project-reflection
          (fn [memory]
            (assoc memory
                   :id (str "urn:adam:reflection:" user-uuid ":" pi-session-id
                            ":" (:memory-id memory))))]
      {:extractor-version extractor-version
       :user-id (identity/user-urn user-uuid)
       :session-id session-id
       :pi-session-id pi-session-id
       :repository repository
       :files (->> (vals @files) (sort-by :relative-path) vec)
       :entry-file-evidence (->> (vals @evidence-by-entry)
                                 (sort-by (juxt :entry-id :file-id))
                                 vec)
       :observations (mapv project-observation (:observations memory-projection))
       :reflections (mapv project-reflection (:reflections memory-projection))
       :memory-diagnostics (vec (:diagnostics memory-projection))})))
