(ns adam.derivation.pi-test
  (:require [adam.derivation.config :as config]
            [adam.derivation.pi :as pi]
            [cljs.test :refer-macros [deftest is async]]))

(deftest registry-adapter-resolves-and-executes-an-isolated-request
  (async done
    (let [model #js {:provider "fake" :id "session" :contextWindow 200000 :maxTokens 16384 :headers #js {:authorization "PRIVATE"}}
          override #js {:provider "fake" :id "override" :contextWindow 100000 :maxTokens 8192}
          attempt (fn [override?]
                    (let [calls (atom []) registry #js {:find (fn [provider id] (is (= ["fake" "override"] [provider id])) override)
                                                       :streamSimple (fn [model context options]
                                                                       (swap! calls conj [model context options])
                                                                       #js {:result (fn [] (js/Promise.resolve #js {:stopReason "stop" :content #js [#js {:type "text" :text "{\"conclusions\":[],\"links\":[]}"}]}))})}
                          ctx #js {:model model :modelRegistry registry :messages #js ["PRIVATE_CONVERSATION"] :tools #js ["write"]}
                          adapter (pi/provider ctx (when override? {:model {:provider "fake" :id "override"}}))
                          controller (js/AbortController.)]
                      (is (= (if override? "override" "session") (get-in adapter [:model :id])))
                      (is (= #{:provider :id :context-window :max-output-tokens} (set (keys (:model adapter)))))
                      (-> ((:execute! adapter) {:model (:model adapter) :payload "ISOLATED_PAYLOAD" :max-output-tokens 4096 :signal (.-signal controller)})
                          (.then (fn [result]
                                   (is (= {:finish-reason :stop :conclusions [] :links []} result))
                                   (is (= 1 (count @calls)))
                                   (let [[actual ^js context ^js options] (first @calls)]
                                     (is (identical? (if override? override model) actual))
                                     (is (= 1 (alength (.-messages context))))
                                     (is (= "ISOLATED_PAYLOAD" (.-content (aget (.-messages context) 0))))
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
                    (let [registry #js {:streamSimple (fn [_ _ _] #js {:result (fn [] (js/Promise.resolve #js {:stopReason reason :errorMessage "PRIVATE_PROVIDER_ERROR" :content #js []}))})}
                          adapter (pi/provider #js {:model model :modelRegistry registry} nil)]
                      (-> ((:execute! adapter) {:model (:model adapter) :payload "{}" :max-output-tokens 8192})
                          (.then (fn [_] (is false "incomplete provider output must fail"))
                                 (fn [error]
                                   (is (some? (config/error-code error)))
                                   (is (not (.includes (.-message error) "PRIVATE_PROVIDER_ERROR"))))))))]
      (-> (js/Promise.all (to-array (map attempt ["length" "error" "aborted" "toolUse"])))
          (.catch (fn [error] (is false (.-stack error)))) (.finally done)))))

(deftest registry-adapter-validates-before-dispatch-and-sanitizes-throws
  (async done
    (let [model #js {:provider "fake" :id "session" :contextWindow 200000 :maxTokens 8192}
          calls (atom 0)
          registry #js {:find (fn [_ _] nil)
                        :streamSimple (fn [_ _ _] (swap! calls inc) (throw (js/Error. "PRIVATE_PROVIDER_ERROR")))}
          ctx #js {:model model :modelRegistry registry}
          adapter (pi/provider ctx nil)
          base {:model (:model adapter) :payload "{}" :max-output-tokens 8192}
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
