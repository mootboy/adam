(ns adam.knowledge.surfaces
  (:require [adam.knowledge.query :as query]
            [clojure.string :as string]))

(def ^:private command-limit 20)
(def ^:private tool-result-limit 10)
(def ^:private tool-query-limit (inc tool-result-limit))
(def ^:private output-max-lines 200)
(def ^:private output-content-lines (- output-max-lines 2))
(def ^:private output-max-bytes 12000)
(def ^:private output-content-bytes 11400)

(defn- invoke [object method & arguments]
  (.apply (aget object method) object (to-array arguments)))

(defn- notify! [ctx message level]
  (when-let [ui (aget ctx "ui")]
    (invoke ui "notify" message level)))

(defn- error-message [error]
  (or (.-message error) (str error)))

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

(defn truncate-tool-output [text]
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

(defn- command-request [arguments cwd]
  (let [arguments (string/trim (or arguments ""))]
    (if (string/starts-with? arguments "--origin")
      (when-let [[_ origin path] (re-matches #"--origin\s+(\S+)\s+(.+)" arguments)]
        {:cwd cwd :origin origin :path (string/trim path) :limit command-limit})
      (when-not (string/blank? arguments)
        {:cwd cwd :path arguments :limit command-limit}))))

(defn register-command! [pi dependencies]
  (invoke
   pi
   "registerCommand"
   "adam:context"
   #js {:description "Show observational memories linked to a repository file"
        :handler
        (fn [arguments ctx]
          (if-let [request (command-request arguments (aget ctx "cwd"))]
              (-> (query/query-file-memory!
                   dependencies
                   request)
                  (.then
                   (fn [{:keys [relative-path memories]}]
                     (if (empty? memories)
                       (notify! ctx
                                (str "No code-linked memories found for " relative-path)
                                "info")
                       (notify! ctx
                                (query/render-file-memory-results
                                 "adam context" relative-path memories)
                                "info"))))
                  (.catch
                   (fn [error]
                     (notify!
                      ctx
                      (case (query/query-error-code error)
                        :repository-not-found
                        "adam context unavailable: path is not in a Git repository"
                        :path-outside-repository
                        "adam context path must identify a file inside the resolved Git repository"
                        :invalid-origin
                        "adam context origin must be a valid Git remote"
                        :invalid-origin-path
                        "adam context origin lookup requires a normalized repository-relative path"
                        (str "adam context query failed: " (error-message error)))
                      "error"))))
              (notify! ctx
                       "Usage: /adam:context <path> | /adam:context --origin <git-origin> <relative-path>"
                       "error")))}))

(defn register-tool! [pi dependencies]
  (invoke
   pi
   "registerTool"
   #js {:name "adam_file_context"
        :label "adam file context"
        :description
        (str "Retrieve observational memories deterministically linked to a repository file. "
             "Use this before changing a file when prior decisions or rationale may matter. "
             "Accepts local paths, or an optional Git origin with a repository-relative path; "
             "it is not semantic search.")
        :promptSnippet
        "Use adam_file_context({ path, origin? }) to retrieve prior observational memories linked to a known repository file."
        :promptGuidelines
        #js ["Use adam_file_context when prior decisions or rationale associated with a specific file could materially affect the work."
             "Do not call it for every file or use it as semantic or global search; provide a specific path."]
        :parameters
        #js {:type "object"
             :required #js ["path"]
             :additionalProperties false
             :properties
             #js {:path #js {:type "string"
                             :minLength 1
                             :description
                             "Repository-relative, workspace-relative, or absolute file path."}
                  :origin #js {:type "string"
                               :minLength 1
                               :description
                               "Optional Git origin. When supplied, path must be normalized and repository-relative."}}}
        :execute
        (fn [_tool-call-id parameters _signal _on-update ctx]
          (let [origin (aget parameters "origin")
                request (cond-> {:cwd (aget ctx "cwd")
                                 :path (aget parameters "path")
                                 :limit tool-query-limit}
                          (some? origin) (assoc :origin origin))]
          (-> (query/query-file-memory!
               dependencies
               request)
              (.then
               (fn [{:keys [repository relative-path memories]}]
                 (let [shown (vec (take tool-result-limit memories))
                       item-truncated? (> (count memories) (count shown))
                       details #js {:status (if (empty? shown) "empty" "ok")
                                    :path relative-path
                                    :repositoryId (:id repository)
                                    :resultCount (count shown)
                                    :truncated item-truncated?}]
                   (if (empty? memories)
                     #js {:content #js [#js {:type "text"
                                            :text (str "No code-linked memories found for "
                                                       relative-path)}]
                          :details details}
                     (let [base (query/render-file-memory-results
                                 "adam file context" relative-path shown)
                           rendered (if item-truncated?
                                      (str base "\n\n[" tool-result-limit
                                           " file memories shown; additional results omitted.]")
                                      base)
                           truncated (truncate-tool-output rendered)]
                       (when (:truncated? truncated)
                         (aset details "truncated" true))
                       #js {:content #js [#js {:type "text" :text (:content truncated)}]
                            :details details}))))))))}))
