(ns adam.replica.neo4j-test
  (:require [adam.knowledge.store :as knowledge-store]
            [adam.replica.neo4j :as neo4j]
            [adam.replica.store :as store]
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

(defn- fake-record [values]
  #js {:get (fn [key] (get values key))})

(defn- transactional-driver
  ([] (transactional-driver false))
  ([mismatch?]
   (let [calls (atom [])
        closes (atom 0)
        tx #js {:run
                (fn [query params]
                  (swap! calls conj {:query query :params params})
                  (cond
                    (re-find #"RETURN e.entryId" query)
                    (let [^js input (first (array-seq (.-entries params)))]
                      (js/Promise.resolve
                       #js {:records
                            #js [(fake-record
                                  {"entryId" (.-entryId input)
                                   "expectedHash" (if mismatch? "stored-hash" (.-payloadHash input))
                                   "actualHash" (.-payloadHash input)})]}))

                    (re-find #"RETURN s.completeThroughByteOffset AS byteOffset" query)
                    (js/Promise.resolve
                     #js {:records #js [(fake-record {"byteOffset" (.-byteOffset params)})]})

                    :else
                    (js/Promise.resolve #js {:records #js []})))}
        session #js {:executeWrite (fn [work] (work tx))
                     :close (fn []
                              (swap! closes inc)
                              (js/Promise.resolve nil))}
        driver #js {:session (fn [_] session)}]
     {:driver driver :calls calls :closes closes})))

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

(deftest writes-entry-tree-and-checkpoint-in-one-transaction
  (async done
    (let [{:keys [driver calls closes]} (transactional-driver)
          replica (neo4j/replica-with-driver driver "neo4j")
          request {:session {:id "urn:adam:session:user-1:session-1"
                             :user-id "urn:adam:user:user-1"
                             :pi-session-id "session-1"
                             :header-json "{\"type\":\"session\"}"
                             :current-leaf-id "entry-1"
                             :source-file "/sessions/session.jsonl"
                             :writer-version "adam-v1"}
                   :entries [{:id "urn:adam:entry:user-1:session-1:entry-1"
                              :entry-id "entry-1"
                              :type "message"
                              :parent-id nil
                              :ordinal 0
                              :raw-json "{\"id\":\"entry-1\"}"
                              :payload-hash "hash-1"
                              :payload-bytes 22}]
                   :checkpoint {:complete-through-ordinal 0
                                :complete-through-byte-offset 100
                                :committed-prefix-hash "prefix-1"}}]
      (-> (store/write-batch! replica request)
          (.then
           (fn [_]
             (let [queries (mapv :query @calls)
                   entry-call (first (filter #(re-find #"RETURN e.entryId" (:query %))
                                             @calls))
                   ^js input (first (array-seq (.-entries (:params entry-call))))]
               (is (= "{\"id\":\"entry-1\"}" (.-rawJson input)))
               (is (some #(re-find #"FORKED_FROM" %) queries))
               (is (some #(re-find #"HAS_ENTRY" %) queries))
               (is (some #(re-find #"PARENT" %) queries))
               (is (some #(re-find #"CURRENT_LEAF" %) queries))
               (is (= 1 @closes))
               (done))))
          (.catch
           (fn [error]
             (is false (.-stack error))
             (done)))))))

(deftest rejects-immutable-entry-payload-mismatches
  (async done
    (let [{:keys [driver closes]} (transactional-driver true)
          replica (neo4j/replica-with-driver driver "neo4j")
          request {:session {:id "urn:adam:session:user-1:session-1"
                             :user-id "urn:adam:user:user-1"
                             :pi-session-id "session-1"
                             :header-json "{}"
                             :current-leaf-id "entry-1"
                             :source-file "/sessions/session.jsonl"
                             :writer-version "adam-v1"}
                   :entries [{:id "urn:adam:entry:user-1:session-1:entry-1"
                              :entry-id "entry-1"
                              :type "message"
                              :parent-id nil
                              :ordinal 0
                              :raw-json "{}"
                              :payload-hash "incoming-hash"
                              :payload-bytes 2}]
                   :checkpoint {:complete-through-ordinal 0
                                :complete-through-byte-offset 10
                                :committed-prefix-hash "prefix"}}]
      (-> (store/write-batch! replica request)
          (.then
           (fn [_]
             (is false "expected immutable-entry conflict")
             (done)))
          (.catch
           (fn [error]
             (is (store/immutable-entry-conflict? error))
             (is (= "entry-1" (:entry-id (ex-data error))))
             (is (= "stored-hash" (:expected-hash (ex-data error))))
             (is (= 1 @closes))
             (done)))))))

(deftest completes-a-session-only-at-the-committed-checkpoint
  (async done
    (let [{:keys [driver calls closes]} (transactional-driver)
          replica (neo4j/replica-with-driver driver "neo4j")
          request {:session {:id "urn:adam:session:user-1:session-1"
                             :user-id "urn:adam:user:user-1"
                             :pi-session-id "session-1"
                             :header-json "{}"
                             :current-leaf-id nil
                             :source-file "/sessions/session.jsonl"
                             :writer-version "adam-v1"}
                   :checkpoint {:complete-through-ordinal -1
                                :complete-through-byte-offset 42
                                :committed-prefix-hash "header-hash"
                                :entry-count 0
                                :log-hash "log-hash"}
                   :log-bytes 42
                   :largest-entry-bytes 0
                   :has-final-newline? true}]
      (-> (store/complete-session! replica request)
          (.then
           (fn [_]
             (let [completion-call
                   (first
                    (filter #(re-find #"s.entryCount = \$entryCount" (:query %))
                            @calls))
                   ^js params (:params completion-call)]
               (is (= 0 (.-entryCount params)))
               (is (= "log-hash" (.-logHash params)))
               (is (= 42 (.-logBytes params)))
               (is (some #(re-find #"CURRENT_LEAF" (:query %)) @calls))
               (is (= 1 @closes))
               (done))))
          (.catch
           (fn [error]
             (is false (.-stack error))
             (done)))))))


(deftest initializes-file-evidence-constraints-separately
  (async done
    (let [{:keys [driver calls closes]} (recording-driver)
          replica (neo4j/replica-with-driver driver "neo4j")]
      (-> (knowledge-store/ensure-file-evidence-schema! replica)
          (.then
           (fn [_]
             (let [queries (mapv :query (filter :query @calls))]
               (is (= 2 (count queries)))
               (is (re-find #"AdamRepository" (first queries)))
               (is (re-find #"AdamCodeFile" (second queries)))
               (is (= 1 @closes))
               (done))))
          (.catch
           (fn [error]
             (is false (.-stack error))
             (done)))))))

(deftest replaces-derived-file-evidence-with-revision-provenance
  (async done
    (let [{:keys [driver calls closes]} (transactional-driver)
          replica (neo4j/replica-with-driver driver "neo4j")
          projection {:extractor-version 1
                      :user-id "urn:adam:user:user-1"
                      :session-id "urn:adam:session:user-1:session-1"
                      :pi-session-id "session-1"
                      :repository {:id "urn:adam:repository:user-1:hash"
                                   :root "/repo"
                                   :normalized-remote "github.com/AloiAI/adam"
                                   :commit "feature-head"
                                   :branch "feature/a"
                                   :dirty? true}
                      :files [{:id "urn:adam:file:file-hash"
                               :repository-id "urn:adam:repository:user-1:hash"
                               :relative-path "src/a.cljs"}]
                      :entry-file-evidence
                      [{:entry-id "entry-1" :file-id "urn:adam:file:file-hash"
                        :commit "feature-head" :branch "feature/a" :dirty? true}]}]
      (-> (knowledge-store/index-file-evidence! replica projection)
          (.then
           (fn [_]
             (let [queries (mapv :query @calls)
                   evidence-call (first (filter #(re-find #"UNWIND \$evidence" (:query %)) @calls))
                   ^js evidence (first (array-seq (aget (:params evidence-call) "evidence")))]
               (is (some #(re-find #"DELETE touch" %) queries))
               (is (some #(re-find #"WORKED_ON" %) queries))
               (is (some #(re-find #"AdamCodeFile" %) queries))
               (is (= "feature-head" (.-commit evidence)))
               (is (= "feature/a" (.-branch evidence)))
               (is (= true (.-dirty evidence)))
               (is (= 1 @closes))
               (done))))
          (.catch
           (fn [error]
             (is false (.-stack error))
             (done)))))))
