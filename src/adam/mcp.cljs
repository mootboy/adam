(ns adam.mcp
  (:require [adam.derivation.read :as saved-read]
            [adam.derivation.config :as derivation-config]
            [adam.knowledge.runtime :as runtime]
            [adam.knowledge.tool :as tool]
            [adam.replica.config :as config]
            ["@modelcontextprotocol/sdk/server/mcp.js" :refer [McpServer]]
            ["@modelcontextprotocol/sdk/server/stdio.js" :refer [StdioServerTransport]]
            ["zod" :refer [z]]))

(defn- error-message [error]
  (or (.-message error) (str error)))

(defn start [version]
  (let [resolved-config (config/resolve-process-config)]
    (when-not (:enabled? resolved-config)
      (throw (js/Error. (str "adam MCP unavailable: " (:reason resolved-config)))))
    (let [{:keys [dependencies close!]} (runtime/create-query-runtime resolved-config)
          server (McpServer. #js {:name "adam" :version version})
          transport (StdioServerTransport.)
          input-schema #js {:path (-> (.string z) (.min 1))
                            :origin (-> (.string z) (.min 1) (.optional))}
          saved-dependencies (saved-read/from-query-runtime dependencies)
          active-requests (atom 0)
          active-saved-controllers (atom #{})
          input-ended? (atom false)
          runtime-close-promise (atom nil)
          server-close-promise (atom nil)]
      (letfn [(close-runtime! []
                (or @runtime-close-promise
                    (let [promise (close!)]
                      (reset! runtime-close-promise promise)
                      promise)))
              (close-server! []
                (or @server-close-promise
                    (let [promise (-> (close-runtime!)
                                      (.then (fn [_] (.close server))))]
                      (reset! server-close-promise promise)
                      promise)))
              (close-if-idle! []
                (when (and @input-ended? (zero? @active-requests))
                  (close-server!)))
              (finish-request! []
                (swap! active-requests dec)
                (close-if-idle!))]
        (set! (.-onclose (.-server server)) close-runtime!)
        ;; The SDK may dispatch buffered JSON-RPC messages just after stdin emits end.
        (.once js/process.stdin "end"
               (fn []
                 (js/setTimeout
                  (fn []
                    (reset! input-ended? true)
                    (doseq [controller @active-saved-controllers] (.abort controller))
                    (close-if-idle!))
                  100)))
        (.registerTool
         server
         "adam_file_context"
         #js {:title "Adam file context"
              :description tool/description
              :inputSchema input-schema
              :annotations #js {:readOnlyHint true
                                :destructiveHint false
                                :idempotentHint true
                                :openWorldHint false}}
         (fn [parameters]
           (swap! active-requests inc)
           (let [origin (aget parameters "origin")
                 request (cond-> {:cwd (.cwd js/process)
                                  :path (aget parameters "path")}
                           (some? origin) (assoc :origin origin))]
             (-> (tool/execute! dependencies request)
                 (.then
                  (fn [{:keys [content]}]
                    #js {:content #js [#js {:type "text" :text content}]}))
                 (.catch
                  (fn [error]
                    #js {:isError true
                         :content #js [#js {:type "text"
                                           :text (str "adam file context query failed: "
                                                      (error-message error))}]}))
                 (.finally finish-request!)))))
        (.registerTool server "adam_derivations"
          #js {:title "Adam saved derivations" :description saved-read/description
               :inputSchema (-> (.object z #js {:name (-> (.string z) (.optional))
                                 :repository (-> (.string z) (.optional))
                                 :session (-> (.string z) (.optional))
                                 :language (-> (.string z) (.optional))
                                 :history (-> (.boolean z) (.optional))
                                 :limit (-> (.number z) (.int) (.min 1) (.max 20) (.optional))}) (.strict))
               :outputSchema (-> (.object z #js {:status (.enum z #js ["ok" "empty"])
                                                 :result-count (-> (.number z) (.int) (.min 0) (.max 20))
                                                 :truncated? (.boolean z)
                                                 :configuration-available? (.boolean z)
                                                 :configuration-status (.enum z #js ["available" "not-configured" "invalid-config"])}) (.strict))
               :annotations #js {:readOnlyHint true :destructiveHint false :idempotentHint true :openWorldHint false}}
          (fn [parameters extra]
            (swap! active-requests inc)
            (let [controller (js/AbortController.) external (aget extra "signal")
                  signal (if external (js/AbortSignal.any #js [external (.-signal controller)]) (.-signal controller))]
              (swap! active-saved-controllers conj controller)
              (when @input-ended? (.abort controller))
              (-> (saved-read/execute! saved-dependencies (js->clj parameters :keywordize-keys true) signal)
                (.then (fn [{:keys [content details]}]
                         #js {:content #js [#js {:type "text" :text content}] :structuredContent (clj->js details)}))
                (.catch (fn [error]
                          #js {:isError true :content #js [#js {:type "text"
                                                              :text (str "Saved derivation read: " (name (or (derivation-config/error-code error) :unavailable)))}]}))
                (.finally (fn [] (swap! active-saved-controllers disj controller) (finish-request!)))))))
        (-> (.connect server transport)
            (.then
             (fn [_]
               #js {:server server
                    :close close-server!})))))))
