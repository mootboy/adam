(ns adam.knowledge.evidence
  (:require [adam.replica.identity :as identity]
            [clojure.string :as string]
            ["node:crypto" :refer [createHash]]
            ["node:path" :as node-path]))

(def extractor-version 3)
(def pi-implicit-stream "main")
(def ^:private max-memory-diagnostics 100)
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

(defn repository-from-origin [remote]
  (when-let [normalized-remote (normalize-git-remote remote)]
    {:id (str "urn:adam:repository:git:" (sha256 normalized-remote))
     :normalized-remote normalized-remote}))

(defn build-repository
  [{:keys [user-uuid root remote commit branch dirty? worktrees]}]
  (let [root (.resolve node-path root)
        origin-repository (repository-from-origin remote)
        normalized-remote (:normalized-remote origin-repository)
        repository-id (or (:id origin-repository)
                          (str "urn:adam:repository:local:" user-uuid ":" (sha256 root)))
        normalize-worktree
        (fn [worktree]
          (cond-> (assoc worktree :root (.resolve node-path (:root worktree)))
            (string/blank? (:branch worktree)) (dissoc :branch)))
        worktrees (mapv normalize-worktree
                        (or worktrees
                            [{:root root :commit commit :branch branch :dirty? dirty?}]))]
    (cond-> {:id repository-id
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

(defn resolve-origin-file [repository path]
  (let [path (some-> path string/trim)
        posix-path (.-posix node-path)]
    (when (and (not (string/blank? path))
               (not (string/includes? path "\\"))
               (not (.isAbsolute posix-path path))
               (not (re-find #"^[A-Za-z]:/" path))
               (= path (.normalize posix-path path))
               (not= "." path)
               (inside-relative-path? path))
      (let [file-hash (sha256 (str (:id repository) "\u0000" path))]
        {:id (str "urn:adam:file:" file-hash)
         :repository-id (:id repository)
         :relative-path path}))))

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

(defn- unresolved-reference-diagnostics [available observations reflections]
  (let [observation-keys (set (map (juxt :producer :memory-id) observations))
        source-diagnostics
        (for [observation observations
              source-entry (:source-entries observation)
              :when (not (contains? available
                                    [(:stream-id source-entry) (:entry-id source-entry)]))]
          {:producer (:producer observation)
           :memory-id (:memory-id observation)
           :stream-id (:stream-id source-entry)
           :entry-id (:entry-id source-entry)
           :reason :unresolved-reference})
        support-diagnostics
        (for [reflection reflections
              observation-id (:supporting-observation-ids reflection)
              :when (not (contains? observation-keys
                                    [(:producer reflection) observation-id]))]
          {:producer (:producer reflection)
           :memory-id (:memory-id reflection)
           :observation-id observation-id
           :reason :unresolved-reference})]
    (vec (concat source-diagnostics support-diagnostics))))

(defn extract-projection
  [{:keys [user-uuid pi-session-id session-id source-kind source-session-id
           cwd repository entries memory-projection]
    :as input}]
  (let [source-kind (or source-kind identity/pi-source-kind)
        source-session-id (or source-session-id pi-session-id)
        selected (selected-entries entries
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
                                  :stream-id pi-implicit-stream
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
            (let [citation-entry-ids
                  (or (seq (:source-entry-ids memory))
                      (map :entry-id (:source-entries memory)))
                  file-ids
                  (->> citation-entry-ids
                       (mapcat (fn [entry-id]
                                 (keep (fn [[[evidence-entry-id file-id] _]]
                                         (when (= entry-id evidence-entry-id) file-id))
                                       @evidence-by-entry)))
                       distinct
                       sort
                       vec)]
              (assoc memory
                     :id (identity/observation-urn
                          user-uuid source-kind source-session-id
                          (:producer memory) (:memory-id memory))
                     :source-kind source-kind
                     :source-session-id source-session-id
                     :source-entries
                     (or (:source-entries memory)
                         (mapv (fn [entry-id]
                                 {:stream-id pi-implicit-stream :entry-id entry-id})
                               (:source-entry-ids memory)))
                     :file-ids file-ids)))
          project-reflection
          (fn [memory]
            (assoc memory
                   :id (identity/reflection-urn
                        user-uuid source-kind source-session-id
                        (:producer memory) (:memory-id memory))
                   :source-kind source-kind
                   :source-session-id source-session-id))
          observations (mapv project-observation (:observations memory-projection))
          reflections (mapv project-reflection (:reflections memory-projection))
          available (set (map (fn [entry]
                                [pi-implicit-stream (:entry-id entry)])
                              entries))]
      {:extractor-version extractor-version
       :user-id (identity/user-urn user-uuid)
       :session-id session-id
       :pi-session-id pi-session-id
       :repository repository
       :files (->> (vals @files) (sort-by :relative-path) vec)
       :entry-file-evidence (->> (vals @evidence-by-entry)
                                 (sort-by (juxt :entry-id :file-id))
                                 vec)
       :available-source-entries available
       :observations observations
       :reflections reflections
       :memory-diagnostics
       (->> (concat (:diagnostics memory-projection)
                    (unresolved-reference-diagnostics available observations reflections))
            (take max-memory-diagnostics) vec)})))

(defn attach-memory-projection [projection user-uuid memory-projection]
  (let [source-kind (:source-kind projection)
        source-session-id (:source-session-id projection)
        evidence-by-entry
        (group-by (fn [item]
                    [(or (:stream-id item) pi-implicit-stream) (:entry-id item)])
                  (:entry-file-evidence projection))
        project-observation
        (fn [memory]
          (let [source-entries
                (or (:source-entries memory)
                    (mapv (fn [entry-id]
                            {:stream-id pi-implicit-stream :entry-id entry-id})
                          (:source-entry-ids memory)))
                file-ids (->> source-entries
                              (mapcat #(get evidence-by-entry
                                            [(:stream-id %) (:entry-id %)]))
                              (map :file-id)
                              distinct sort vec)]
            (assoc memory
                   :id (identity/observation-urn
                        user-uuid source-kind source-session-id
                        (:producer memory) (:memory-id memory))
                   :source-kind source-kind
                   :source-session-id source-session-id
                   :source-entries source-entries
                   :source-entry-ids (mapv :entry-id source-entries)
                   :file-ids file-ids)))
        project-reflection
        (fn [memory]
          (assoc memory
                 :id (identity/reflection-urn
                      user-uuid source-kind source-session-id
                      (:producer memory) (:memory-id memory))
                 :source-kind source-kind
                 :source-session-id source-session-id))
        observations (mapv project-observation (:observations memory-projection))
        reflections (mapv project-reflection (:reflections memory-projection))
        diagnostics
        (->> (concat (:diagnostics memory-projection)
                     (unresolved-reference-diagnostics
                      (:available-source-entries projection) observations reflections))
             (take max-memory-diagnostics) vec)]
    (assoc projection
           :observations observations
           :reflections reflections
           :memory-diagnostics diagnostics)))
