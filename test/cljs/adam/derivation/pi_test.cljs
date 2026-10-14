(ns adam.derivation.pi-test
  (:require [adam.derivation.config :as config]
            [adam.derivation.pi :as pi]
            [adam.derivation.core :as core]
            [adam.derivation.store :as store]
            [cljs.spec.alpha :as s]
            ["node:buffer" :refer [Buffer]]
            [cljs.test :refer-macros [deftest is async]]))

(deftest depth-is-a-command-parameter-and-invalid-values-fail-before-io
  (doseq [depth ["full" "active" "observations" "reflections"]]
    (is (= {:name "preferences" :depth depth :language "cljs"}
           (pi/parse-command (str "preferences --depth " depth " --language cljs")))))
  (doseq [arguments ["--depth" "--depth shallow" "--depth full --depth active"]]
    (try (pi/parse-command arguments) (is false "invalid depth must be rejected")
         (catch :default error (is (= :invalid-selector (config/error-code error)))))))

(deftest stock-codex-sizing-is-a-labelled-local-estimate-with-no-provider-call
  (let [calls (atom 0)
        ctx #js {:model #js {:provider "openai-codex" :id "gpt-6.1-sol" :api "openai-codex-responses" :contextWindow 500000 :maxTokens 128000}
                 :modelRegistry #js {:hasConfiguredAuth (constantly true) :streamSimple (fn [& _] (swap! calls inc))}}
        adapter (pi/provider ctx {:limits {:max-output-tokens 4096}})
        payload (js/JSON.stringify #js {:instructions "Follow the evidence." :output-schema #js {} :configuration #js {}
                                       :memories #js [#js {:content (apply str (repeat 1000 "prefer simple boundaries <|endoftext|> "))}]})
        measured ((:measure-input adapter) payload (:model adapter))]
    (is (= "local-o200k-estimate" (:measurement-method (meta (:measure-input adapter)))))
    (is (pos? measured))
    (is (< measured (.byteLength Buffer payload "utf8")))
    (is (zero? @calls))))

(deftest tokenizer-families-and-unknown-providers-are-explicit-and-metadata-persists
  (async done
    (let [recipes (config/parse-config "derivations:\n  preferences:\n    version: 1\n    guidance: Preserve scope.\n")
          recipe (get recipes "preferences")
          cases [{:provider "openai" :id "gpt-4" :method "local-cl100k-estimate"}
                 {:provider "openai" :id "gpt-4o" :method "local-o200k-estimate"}
                 {:provider "custom" :id "anything" :method "utf8-byte-upper-bound"}]
          adapters (mapv (fn [{:keys [provider id method]}]
                           (let [adapter (pi/provider #js {:model #js {:provider provider :id id :contextWindow 30000 :maxTokens 8192}
                                                          :modelRegistry #js {:hasConfiguredAuth (constantly true) :streamSimple (fn [& _] (is false "no provider dispatch"))}} recipe)]
                             (is (= method (:measurement-method (meta (:measure-input adapter))))) adapter)) cases)]
      (-> (js/Promise.all
            (to-array (map (fn [adapter]
                             (core/refresh! {:measure-input (:measure-input adapter)
                                             :execute! (fn [_] {:finish-reason :stop :conclusions [] :links []})}
                                            {:reader-id "urn:adam:user:test" :name "preferences" :filter {:type :none}
                                             :recipe recipe :model (:model adapter) :memories [] :grants []})) adapters)))
          (.then (fn [results]
                   (doseq [[result expected] (map vector (array-seq results) cases)]
                     (is (= (:method expected) (get-in result [:run :measurement-method])))
                     (is (= (:method expected) (aget (js/JSON.parse (core/snapshot-json (:run result))) "measurement-method"))))))
          (.catch (fn [_] (is false "local sizing must complete")))
          (.finally done)))))

(def payload
  (core/snapshot-json {:instructions "FIXED_POLICY" :output-schema {:type "object"}
                       :configuration {:guidance "TRUSTED_GUIDANCE"}
                       :output-rules {:subject-ids {:personal ["UNTRUSTED_IDENTIFIER"]}}
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
                                     (is (not (.includes (.-systemPrompt context) "UNTRUSTED_IDENTIFIER")))
                                     (is (.includes (.-content (aget (.-messages context) 0)) "UNTRUSTED_IDENTIFIER"))
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

(deftest missing-auth-fails-before-dispatch
  (let [model #js {:provider "fake" :id "session" :contextWindow 200000 :maxTokens 8192}
        registry #js {:hasConfiguredAuth (constantly false) :streamSimple (fn [& _] (is false "must not dispatch"))}
        ctx #js {:model model :modelRegistry registry}]
    (try (pi/provider ctx nil) (is false "auth required")
         (catch :default error (is (= :invalid-model (config/error-code error)))))
    (is (false? (.hasConfiguredAuth registry model)))))

(deftest pi-owned-api-selection-does-not-block-shared-sizing-or-dispatch
  (async done
    (let [attempt (fn [api]
                    (let [calls (atom 0)
                          model #js {:provider "fake" :id "session" :api api :contextWindow 200000 :maxTokens 8192}
                          registry #js {:hasConfiguredAuth (constantly true)
                                        :streamSimple (fn [_ _ ^js options]
                                                        (swap! calls inc)
                                                        (is (= 2048 (.-maxTokens options)) "allowance is requested through Pi")
                                                        #js {:result (fn [] (js/Promise.resolve #js {:stopReason "stop" :content #js [#js {:type "text" :text "{\"conclusions\":[],\"links\":[]}"}]}))})}
                          adapter (pi/provider #js {:model model :modelRegistry registry} nil)]
                      (is (pos? ((:measure-input adapter) payload (:model adapter))))
                      (try ((:measure-input adapter) payload (assoc (:model adapter) :id "wrong"))
                           (is false "measurement must match resolved model")
                           (catch :default error (is (= :invalid-budget (config/error-code error)))))
                      (-> ((:execute! adapter) {:model (:model adapter) :payload payload :max-output-tokens 2048})
                          (.then (fn [result] (is (= [] (:conclusions result))) (is (= 1 @calls)))))))]
      (-> (js/Promise.resolve)
          (.then (fn [] (js/Promise.all (to-array (map attempt ["openai-codex-responses" "pi-custom-api"])))) )
          (.catch (fn [error] (is false (.-stack error)))) (.finally done)))))

(deftest pre-validation-rejections-have-content-free-reasons
  (async done
    (let [model #js {:provider "fake" :id "session" :api "pi-owned-api" :contextWindow 200000 :maxTokens 8192}
          recipe (get (config/parse-config "derivations:\n  preferences:\n    version: 1\n    guidance: Preserve scope.\n") "preferences")
          cases [["length" #js [#js {:type "text" :text "PRIVATE_REPLY"}] :incomplete-output]
                 ["stop" #js [#js {:type "toolCall" :name "PRIVATE_TOOL"}] :non-text-output]
                 ["stop" #js [#js {:type "text" :text "```json PRIVATE_REPLY```"}] :response-json]
                 ["stop" #js [#js {:type "text" :text "{\"secret\":\"PRIVATE_REPLY\"}"}] :response-wire-shape]]
          attempt (fn [[stop blocks reason]]
                    (let [registry #js {:hasConfiguredAuth (constantly true)
                                        :streamSimple (fn [_ _ _]
                                                        #js {:result (fn [] (js/Promise.resolve #js {:stopReason stop :content blocks}))})}
                          adapter (pi/provider #js {:model model :modelRegistry registry} recipe)]
                      (-> (core/refresh-configured!
                           {:load-input! (fn [_] {:reader-id "urn:adam:user:synthetic" :memories [] :grants [] :model (:model adapter)})
                            :measure-input (:measure-input adapter) :execute! (:execute! adapter)}
                           {"preferences" recipe} {:name "preferences"})
                          (.then (fn [[outcome]]
                                   (is (= :invalid-output (:code outcome)))
                                   (is (= reason (:validation-reason outcome)))
                                   (is (not (.includes (js/JSON.stringify (clj->js outcome)) "PRIVATE"))))))))]
      (-> (js/Promise.all (to-array (map attempt cases)))
          (.catch (fn [error] (is false (.-stack error)))) (.finally done)))))

(deftest accepted-model-json-is-byte-bounded-even-when-sdk-ignores-token-allowance
  (async done
    (let [attempt (fn [padding]
                    (let [model #js {:provider "fake" :id "session" :api "pi-custom-api" :contextWindow 200000 :maxTokens 8192}
                          registry #js {:hasConfiguredAuth (constantly true)
                                        :streamSimple (fn [_ _ _]
                                                        #js {:result (fn [] (js/Promise.resolve
                                                                             #js {:stopReason "stop" :content #js [#js {:type "text" :text padding}]}))})}
                          adapter (pi/provider #js {:model model :modelRegistry registry} nil)]
                      (-> ((:execute! adapter) {:model (:model adapter) :payload payload :max-output-tokens 2048})
                          (.then (fn [_] (is false "oversized accepted JSON must be rejected"))
                                 (fn [error]
                                   (is (= :invalid-output (config/error-code error)))
                                   (is (= :output-too-large (:validation-reason (ex-data error)))))))))]
      (-> (js/Promise.all (to-array (map attempt [(str (.repeat " " 1048576) "{\"conclusions\":[],\"links\":[]}")
                                                 (.repeat "界" 350000)])))
          (.catch (fn [error] (is false (.-stack error)))) (.finally done)))))
