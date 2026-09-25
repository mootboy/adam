(ns adam.knowledge.surfaces-test
  (:require [adam.knowledge.evidence :as evidence]
            [adam.knowledge.store :as store]
            [adam.knowledge.surfaces :as surfaces]
            [clojure.string :as string]
            [cljs.test :refer [async deftest is]]))

(def user-uuid "00000000-0000-4000-8000-000000000001")
(def repository
  (evidence/build-repository
   {:user-uuid user-uuid :root "/repo"
    :remote "git@github.com:AloiAI/adam.git"
    :commit "abcdef123456" :branch "main" :dirty? false}))

(defrecord SurfaceStore [calls results]
  store/FileMemoryQueryStore
  (query-file-memory! [_ user-id repository-id relative-path limit]
    (swap! calls conj [user-id repository-id relative-path limit])
    (if (= :fail @results)
      (js/Promise.reject (js/Error. "database unavailable"))
      (js/Promise.resolve @results))))

(defn- setup [results]
  (let [commands (atom {})
        tools (atom {})
        calls (atom [])
        memory-store (->SurfaceStore calls results)
        pi #js {:registerCommand (fn [name definition]
                                  (swap! commands assoc name definition))
                :registerTool (fn [definition]
                                (swap! tools assoc (aget definition "name") definition))}
        dependencies {:get-store! #(js/Promise.resolve memory-store)
                      :get-user! #(js/Promise.resolve {:user-uuid user-uuid})
                      :resolve-repository! (fn [_ _] (js/Promise.resolve repository))}]
    (surfaces/register-command! pi dependencies)
    (surfaces/register-tool! pi dependencies)
    {:commands commands :tools tools :calls calls}))

(deftest command-and-tool-share-query-and-rendering
  (async done
    (let [results (atom [{:kind :observation :memory-id "aaaaaaaaaaaa"
                          :content "A useful fact" :pi-session-id "session-1"
                          :source-entry-ids ["entry-1"]
                          :source-contexts [{:commit "def4567890" :branch "feature/a"
                                             :dirty? true}]
                          :dropped? false}])
          {:keys [commands tools calls]} (setup results)
          notifications (atom [])
          ctx #js {:cwd "/repo"
                   :ui #js {:notify (fn [message level]
                                      (swap! notifications conj [message level]))}}
          command (aget (get @commands "adam:context") "handler")
          tool (get @tools "adam_file_context")]
      (-> (command "./src/a.cljs" ctx)
          (.then
           (fn [_]
             (is (re-find #"adam context: src/a.cljs" (ffirst @notifications)))
             ((aget tool "execute") "call-1" #js {:path "src/a.cljs"}
              nil nil #js {:cwd "/repo"})))
          (.then
           (fn [result]
             (is (= "adam file context: src/a.cljs (1 result)"
                    (first (string/split-lines
                            (aget (aget (aget result "content") 0) "text")))))
             (is (= "ok" (aget (aget result "details") "status")))
             (is (= 20 (last (first @calls))))
             (is (= 11 (last (second @calls))))
             (done)))
          (.catch (fn [error] (is false (.-stack error)) (done)))))))

(deftest tool-bounds-items-output-and-recovers-after-failure
  (async done
    (let [memories (mapv (fn [index]
                           {:kind :observation
                            :memory-id (.padStart (.toString index 16) 12 "0")
                            :content (apply str (repeat 1500 "x"))
                            :pi-session-id (str "session-" index)
                            :source-entry-ids [] :source-contexts [] :dropped? false})
                         (range 11))
          results (atom :fail)
          {:keys [tools]} (setup results)
          tool (get @tools "adam_file_context")
          execute #(.call (aget tool "execute") tool
                          "call" #js {:path "src/a.cljs"}
                          nil nil #js {:cwd "/repo"})]
      (-> (execute)
          (.then (fn [_] (is false "expected rejection") (done)))
          (.catch
           (fn [_]
             (reset! results memories)
             (execute)))
          (.then
           (fn [result]
             (let [details (aget result "details")
                   text (aget (aget (aget result "content") 0) "text")]
               (is (= 10 (aget details "resultCount")))
               (is (= true (aget details "truncated")))
               (is (<= (.byteLength js/Buffer text "utf8") 12000))
               (is (<= (count (string/split text #"\n" -1)) 200))
               (is (re-find #"additional results omitted|Output truncated" text)))
             (done)))
          (.catch (fn [error] (is false (.-stack error)) (done)))))))

(deftest empty-tool-result-and-command-path-errors-are-explicit
  (async done
    (let [results (atom [])
          {:keys [commands tools calls]} (setup results)
          notifications (atom [])
          ctx #js {:cwd "/repo"
                   :ui #js {:notify (fn [message level]
                                      (swap! notifications conj [message level]))}}
          command (aget (get @commands "adam:context") "handler")
          tool (get @tools "adam_file_context")]
      (command "" ctx)
      (let [promise (.call (aget tool "execute") tool
                           "call-empty" #js {:path "src/empty.cljs"}
                           nil nil #js {:cwd "/repo"})]
        (-> promise
            (.then
             (fn [result]
               (is (= ["Usage: /adam:context <path>" "error"]
                      (first @notifications)))
               (is (= "No code-linked memories found for src/empty.cljs"
                      (aget (aget (aget result "content") 0) "text")))
               (is (= "empty" (aget (aget result "details") "status")))
               (is (= 1 (count @calls)))
               (done)))
            (.catch (fn [error] (is false (.-stack error)) (done))))))))
