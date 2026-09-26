(ns adam.mcp
  (:require [adam.knowledge.runtime :as runtime]
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
          active-requests (atom 0)
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
        (-> (.connect server transport)
            (.then
             (fn [_]
               #js {:server server
                    :close close-server!})))))))
