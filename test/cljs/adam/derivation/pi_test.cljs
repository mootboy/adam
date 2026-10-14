(ns adam.derivation.pi-test
  (:require [adam.derivation.config :as config]
            [adam.derivation.pi :as pi]
            [adam.derivation.core :as core]
            [adam.derivation.store :as store]
            [cljs.spec.alpha :as s]
            ["node:buffer" :refer [Buffer]]
            [cljs.test :refer-macros [deftest is async]]))

(def payload
  (core/snapshot-json {:instructions "FIXED_POLICY" :output-schema {:type "object"}
                       :configuration {:guidance "TRUSTED_GUIDANCE"}
                       :memories [{:content "ISOLATED_PAYLOAD"}]}))

(deftest registry-adapter-resolves-and-executes-an-isolated-request
  (async done
    (let [model #js {:provider "fake" :id "session" :contextWindow 200000 :maxTokens 16384 :headers #js {:authorization "PRIVATE"}}
          override #js {:provider "fake" :id "override" :contextWindow 100000 :maxTokens 8192}
          attempt (fn [override?]
                    (let [calls (atom []) registry #js {:hasConfiguredAuth (constantly true) :find (fn [provider id] (is (= ["fake" "override"] [provider id])) override)
                                                       :streamSimple (fn [model context options]
                                                                       (swap! calls conj [model context options])
                                                                       #js {:result (fn [] (js/Promise.resolve #js {:stopReason "stop" :content #js [#js {:type "text" :text "{\"conclusions\":[],\"links\":[]}"}]}))})}
                          ctx #js {:model model :modelRegistry registry :messages #js ["PRIVATE_CONVERSATION"] :tools #js ["write"]}
                          adapter (pi/provider ctx (when override? {:model {:provider "fake" :id "override"}}))
                          controller (js/AbortController.)]
                      (is (= (if override? "override" "session") (get-in adapter [:model :id])))
                      (is (= #{:provider :id :context-window :max-output-tokens} (set (keys (:model adapter)))))
                      (-> ((:execute! adapter) {:model (:model adapter) :payload payload :max-output-tokens 4096 :signal (.-signal controller)})
                          (.then (fn [result]
                                   (is (= {:finish-reason :stop :conclusions [] :links []} result))
                                   (is (= 1 (count @calls)))
                                   (let [[actual ^js context ^js options] (first @calls)]
                                     (is (identical? (if override? override model) actual))
                                     (is (= 1 (alength (.-messages context))))
                                     (is (.includes (.-content (aget (.-messages context) 0)) "ISOLATED_PAYLOAD"))
                                     (is (not (.includes (.-systemPrompt context) "ISOLATED_PAYLOAD")))
                                     (is (.includes (.-systemPrompt context) "TRUSTED_GUIDANCE"))
                                     (is (.includes (.-systemPrompt context) "raw JSON only"))
                                     (is (= 0 (.-maxRetries options)))
                                     (is (= "none" (.-cacheRetention options)))
                                     (is (zero? (alength (.-tools context))))
                                     (is (= 4096 (.-maxTokens options)))
                                     (is (identical? (.-signal controller) (.-signal options)))
                                     (is (not (.includes (js/JSON.stringify context) "PRIVATE")))))))))]
      (-> (js/Promise.all #js [(attempt false) (attempt true)])
          (.catch (fn [error] (is false (.-stack error)))) (.finally done)))))

(deftest registry-adapter-rejects-incomplete-or-failed-responses
  (async done
    (let [model #js {:provider "fake" :id "session" :contextWindow 200000 :maxTokens 8192}
          attempt (fn [reason]
                    (let [registry #js {:hasConfiguredAuth (constantly true) :streamSimple (fn [_ _ _] #js {:result (fn [] (js/Promise.resolve #js {:stopReason reason :errorMessage "PRIVATE_PROVIDER_ERROR" :content #js []}))})}
                          adapter (pi/provider #js {:model model :modelRegistry registry} nil)]
                      (-> ((:execute! adapter) {:model (:model adapter) :payload payload :max-output-tokens 8192})
                          (.then (fn [_] (is false "incomplete provider output must fail"))
                                 (fn [error]
                                   (is (s/valid? ::store/failure (config/error-code error)))
                                   (is (not (.includes (.-message error) "PRIVATE_PROVIDER_ERROR"))))))))]
      (-> (js/Promise.all (to-array (map attempt ["length" "error" "aborted" "toolUse" "deferred"])))
          (.catch (fn [error] (is false (.-stack error)))) (.finally done)))))

(deftest registry-adapter-validates-before-dispatch-and-sanitizes-throws
  (async done
    (let [model #js {:provider "fake" :id "session" :contextWindow 200000 :maxTokens 8192}
          calls (atom 0)
          registry #js {:hasConfiguredAuth (constantly true) :find (fn [_ _] nil)
                        :streamSimple (fn [_ _ _] (swap! calls inc) (throw (js/Error. "PRIVATE_PROVIDER_ERROR")))}
          ctx #js {:model model :modelRegistry registry}
          adapter (pi/provider ctx nil)
          base {:model (:model adapter) :payload payload :max-output-tokens 8192}
          expect-failure (fn [request code]
                           (-> ((:execute! adapter) request)
                               (.then (fn [_] (is false "invalid request must fail"))
                                      (fn [error]
                                        (is (= code (config/error-code error)))
                                        (is (not (.includes (.-message error) "PRIVATE")))))))]
      (try (pi/provider ctx {:model {:provider "fake" :id "missing"}})
           (is false "unresolved override must fail")
           (catch :default error (is (= :invalid-model (config/error-code error)))))
      (-> (js/Promise.all (to-array (for [request [(assoc base :max-output-tokens 8193)
                                                  (assoc-in base [:model :id] "different")
                                                  (assoc base :payload nil)]]
                                    (expect-failure request :invalid-model))))
          (.then (fn [_] (is (zero? @calls)) (expect-failure base :executor-failed)))
          (.then (fn [_] (is (= 1 @calls))))
          (.catch (fn [error] (is false (.-stack error)))) (.finally done)))))

(deftest conservative-sizing-and-minimal-reasoning-stay-inside-the-output-reservation
  (async done
    (let [model #js {:provider "fake" :id "reasoning" :api "anthropic-messages" :reasoning true
                     :contextWindow 200000 :maxTokens 8192}
          captured (atom nil)
          registry #js {:hasConfiguredAuth (constantly true)
                        :streamSimple (fn [_ context options]
                                        (reset! captured [context options])
                                        #js {:result (fn [] (js/Promise.resolve
                                                              #js {:stopReason "stop" :content #js [#js {:type "text" :text "{\"conclusions\":[],\"links\":[]}"}]}))})}
          adapter (pi/provider #js {:model model :modelRegistry registry} nil)
          measured ((:measure-input adapter) payload (:model adapter))]
      (is (> measured (.byteLength Buffer "ISOLATED_PAYLOAD" "utf8")))
      (-> (js/Promise.resolve)
          (.then (fn [] ((:execute! adapter) {:model (:model adapter) :payload payload :max-output-tokens 8192})))
          (.then (fn [_]
                   (let [[^js context ^js options] @captured]
                     (is (= measured (+ (.byteLength Buffer (.-systemPrompt context) "utf8")
                                        (.byteLength Buffer (.-content (aget (.-messages context) 0)) "utf8"))))
                     (is (= "minimal" (.-reasoning options)))
                     (is (= 8192 (+ (.-maxTokens options) (.-minimal (.-thinkingBudgets options))))))))
          (.catch (fn [error] (is false (.-stack error)))) (.finally done)))))

(deftest small-budgeted-output-fails-before-graph-or-model-work
  (let [ctx #js {:model #js {:provider "fake" :id "reasoning" :api "anthropic-messages" :reasoning true
                             :contextWindow 200000 :maxTokens 8192}
                 :modelRegistry #js {:hasConfiguredAuth (constantly true)
                                     :streamSimple (fn [& _] (is false "must not dispatch"))}}]
    (doseq [size [1 1024 2047]]
      (try (pi/provider ctx {:limits {:max-output-tokens size}}) (is false "invalid thinking budget")
           (catch :default error (is (= :invalid-budget (config/error-code error))))))))

(deftest missing-auth-or-unsupported-sizing-fails-before-dispatch
  (let [model #js {:provider "fake" :id "session" :contextWindow 200000 :maxTokens 8192}
        registry #js {:hasConfiguredAuth (constantly false) :streamSimple (fn [& _] (is false "must not dispatch"))}
        ctx #js {:model model :modelRegistry registry}]
    (try (pi/provider ctx nil) (is false "auth required")
         (catch :default error (is (= :invalid-model (config/error-code error)))))
    (aset registry "hasConfiguredAuth" (constantly true))
    (let [adapter (pi/provider ctx nil)]
      (try ((:measure-input adapter) payload (:model adapter)) (is false "API-specific bound required")
           (catch :default error (is (= :invalid-budget (config/error-code error))))))))
