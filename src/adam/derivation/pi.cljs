(ns adam.derivation.pi
  (:require [adam.derivation.config :as config]
            [adam.derivation.core :as core]
            [cljs.spec.alpha :as s]))

;; Pi's registry owns provider composition and request-time authentication. Keep
;; its native model private; only credential-free capabilities enter the core.
(defn provider [ctx recipe]
  (let [^js registry (aget ctx "modelRegistry")
        override (:model recipe)
        model (if override
                (when (and registry (= "function" (goog/typeOf (aget registry "find"))))
                  (.find registry (:provider override) (:id override)))
                (aget ctx "model"))
        capabilities (when model {:provider (aget model "provider") :id (aget model "id")
                                  :context-window (aget model "contextWindow")
                                  :max-output-tokens (aget model "maxTokens")})]
    (when-not (and registry (= "function" (goog/typeOf (aget registry "streamSimple")))
                   (s/valid? ::core/model capabilities))
      (config/reject! :invalid-model "a resolved Pi model and registry are required"))
    {:model capabilities
     :execute!
     (fn [{:keys [payload signal max-output-tokens] :as request}]
       (-> (js/Promise.resolve)
           (.then (fn []
                    (when-not (and (= capabilities (:model request)) (string? payload)
                                   (s/valid? ::config/positive-integer max-output-tokens)
                                   (<= max-output-tokens (:max-output-tokens capabilities)))
                      (config/reject! :invalid-model "request must match resolved model and output limits"))
                    (let [^js stream (.streamSimple registry model
                                   #js {:messages #js [#js {:role "user" :content payload :timestamp (.now js/Date)}]
                                        :tools #js []}
                                   #js {:maxTokens max-output-tokens :signal signal})]
                      (.result stream))))
           (.then (fn [message]
                    (when-not (= "stop" (aget message "stopReason"))
                      (config/reject! (case (aget message "stopReason")
                                        "error" :executor-failed "aborted" :cancelled :incomplete-output)
                                      "provider did not complete a derivation response"))
                    (let [blocks (array-seq (aget message "content"))]
                      (when-not (every? #(or (= "thinking" (aget % "type"))
                                            (and (= "text" (aget % "type")) (string? (aget % "text")))) blocks)
                        (config/reject! :invalid-output "derivation response must be text, never tool calls"))
                      (core/decode-response (apply str (keep #(when (= "text" (aget % "type")) (aget % "text")) blocks)) :stop))))
           (.catch (fn [error]
                     (if (config/error-code error) (throw error)
                         (config/reject! :executor-failed "provider request failed"))))))}))
