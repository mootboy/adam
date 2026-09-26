(ns adam.knowledge.surfaces
  (:require [adam.knowledge.query :as query]
            [adam.knowledge.tool :as tool]
            [clojure.string :as string]))

(def ^:private command-limit 20)

(defn- invoke [object method & arguments]
  (.apply (aget object method) object (to-array arguments)))

(defn- notify! [ctx message level]
  (when-let [ui (aget ctx "ui")]
    (invoke ui "notify" message level)))

(defn- error-message [error]
  (or (.-message error) (str error)))

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
        :description tool/description
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
                                 :path (aget parameters "path")}
                          (some? origin) (assoc :origin origin))]
            (-> (tool/execute! dependencies request)
                (.then
                 (fn [{:keys [content details]}]
                   #js {:content #js [#js {:type "text" :text content}]
                        :details
                        #js {:status (:status details)
                             :path (:path details)
                             :repositoryId (:repository-id details)
                             :resultCount (:result-count details)
                             :truncated (:truncated? details)}})))))}))
