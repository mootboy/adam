(ns adam.knowledge.query-test
  (:require [adam.knowledge.evidence :as evidence]
            [adam.knowledge.query :as query]
            [adam.knowledge.store :as store]
            [cljs.test :refer [async deftest is]]))

(def user-uuid "00000000-0000-4000-8000-000000000001")
(def repository
  (evidence/build-repository
   {:user-uuid user-uuid :root "/workspace/repo"
    :remote "git@github.com:AloiAI/adam.git"
    :commit "abcdef123456" :branch "main" :dirty? false}))

(defrecord QueryStore [calls results]
  store/FileMemoryQueryStore
  (query-file-memory! [_ user-id repository-id relative-path limit]
    (swap! calls conj [user-id repository-id relative-path limit])
    (js/Promise.resolve results)))

(deftest resolves-workspace-path-and-queries-canonical-file
  (async done
    (let [calls (atom [])
          memory-store (->QueryStore calls [{:kind :observation}])
          resolved (atom [])
          dependencies
          {:get-store! #(js/Promise.resolve memory-store)
           :get-user! #(js/Promise.resolve {:user-uuid user-uuid})
           :resolve-repository!
           (fn [cwd _user-uuid]
             (swap! resolved conj cwd)
             (js/Promise.resolve
              (when (= cwd "/workspace/repo/src") repository)))}]
      (-> (query/query-file-memory!
           dependencies {:cwd "/workspace" :path "repo/src/a.cljs" :limit 11})
          (.then
           (fn [result]
             (is (= "src/a.cljs" (:relative-path result)))
             (is (= ["/workspace" "/workspace/repo/src"] @resolved))
             (is (= [["urn:adam:user:00000000-0000-4000-8000-000000000001"
                      (:id repository) "src/a.cljs" 11]]
                    @calls))
             (done)))
          (.catch (fn [error] (is false (.-stack error)) (done)))))))

(deftest rejects-outside-path-before-opening-store
  (async done
    (let [store-calls (atom 0)]
      (-> (query/query-file-memory!
           {:get-store! (fn [] (swap! store-calls inc) (js/Promise.resolve nil))
            :get-user! #(js/Promise.resolve {:user-uuid user-uuid})
            :resolve-repository! (fn [_ _] (js/Promise.resolve repository))}
           {:cwd "/workspace/repo" :path "../secret" :limit 10})
          (.then (fn [_] (is false "expected rejection") (done)))
          (.catch
           (fn [error]
             (is (= :path-outside-repository (query/query-error-code error)))
             (is (zero? @store-calls))
             (done)))))))

(deftest renders-memory-content-and-revision-provenance
  (is (= (str "adam context: src/a.cljs (2 results)\n"
              "[observation aaaaaaaaaaaa, dropped] A useful fact\n"
              "  Session: session-1; sources: entry-1\n"
              "  Observed: feature/a @ def4567 (dirty)\n"
              "[reflection bbbbbbbbbbbb] Durable decision\n"
              "  Session: session-1; sources: entry-1")
         (query/render-file-memory-results
          "adam context"
          "src/a.cljs"
          [{:kind :observation :memory-id "aaaaaaaaaaaa" :content "A useful\n fact"
            :pi-session-id "session-1" :source-entry-ids ["entry-1"]
            :source-contexts [{:commit "def4567890" :branch "feature/a" :dirty? true}]
            :dropped? true}
           {:kind :reflection :memory-id "bbbbbbbbbbbb" :content "Durable decision"
            :pi-session-id "session-1" :source-entry-ids ["entry-1"]
            :source-contexts []}]))))
