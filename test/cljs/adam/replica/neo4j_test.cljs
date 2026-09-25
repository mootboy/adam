(ns adam.replica.neo4j-test
  (:require [adam.replica.neo4j :as neo4j]
            [cljs.test :refer [async deftest is]]))

(defn- recording-driver []
  (let [calls (atom [])
        closes (atom 0)
        session #js {:run (fn [query params]
                            (swap! calls conj {:query query
                                               :params (when params
                                                         (js->clj params :keywordize-keys true))})
                            (js/Promise.resolve #js {:records #js []}))
                     :close (fn []
                              (swap! closes inc)
                              (js/Promise.resolve nil))}
        driver #js {:session (fn [options]
                               (swap! calls conj
                                      {:session-options
                                       (js->clj options :keywordize-keys true)})
                               session)}]
    {:driver driver :calls calls :closes closes}))

(deftest initializes-the-fresh-adam-schema-and-user-identity
  (async done
    (let [{:keys [driver calls closes]} (recording-driver)]
      (-> (neo4j/initialize-schema!
           driver
           "neo4j"
           {:id "urn:adam:user:user-1"
            :identity {:id "urn:adam:identity:git-email:hash"
                       :kind "git-email"
                       :value "Linus@Example.com"
                       :normalized-value "linus@example.com"
                       :display-value "Linus@Example.com"}})
          (.then
           (fn [_]
             (let [queries (mapv :query (filter :query @calls))]
               (is (= 6 (count queries)))
               (is (every? #(re-find #"Adam(User|Identity|Session|Entry)" %)
                           (take 4 queries)))
               (is (re-find #"MERGE \(u:AdamUser" (nth queries 4)))
               (is (re-find #"MERGE \(i:AdamIdentity" (nth queries 5)))
               (is (= 2 @closes))
               (done))))
          (.catch
           (fn [error]
             (is false (.-stack error))
             (done)))))))
