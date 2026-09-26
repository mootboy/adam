(ns adam.replica.neo4j-test
  (:require [adam.knowledge.store :as knowledge-store]
            [adam.replica.neo4j :as neo4j]
            [adam.replica.store :as store]
            [cljs.test :refer [async deftest is]]))

(defn- recording-driver []
  (let [calls (atom [])
        closes (atom 0)
        run! (fn [query params]
               (swap! calls conj {:query query
                                  :params (when params
                                            (js->clj params :keywordize-keys true))})
               (js/Promise.resolve #js {:records #js []}))
        tx #js {:run run!}
        session #js {:run run!
                     :executeWrite (fn [work] (work tx))
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
               (is (= 5 (count queries)))
               (is (some #(re-find #"AdamEntry.*sessionId.*entryId" %) queries))
               (is (some #(re-find #"AdamRepository" %) queries))
               (is (some #(re-find #"AdamCodeFile" %) queries))
               (is (some #(re-find #"AdamObservation" %) queries))
               (is (some #(re-find #"AdamReflection" %) queries))
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
          projection {:extractor-version 3
                      :user-id "urn:adam:user:user-1"
                      :session-id "urn:adam:session:user-1:session-1"
                      :pi-session-id "session-1"
                      :repository {:id "urn:adam:repository:git:hash"
                                   :root "/repo"
                                   :normalized-remote "github.com/AloiAI/adam"
                                   :commit "feature-head"
                                   :branch "feature/a"
                                   :dirty? true}
                      :files [{:id "urn:adam:file:file-hash"
                               :repository-id "urn:adam:repository:git:hash"
                               :relative-path "src/a.cljs"}]
                      :entry-file-evidence
                      [{:entry-id "entry-1" :file-id "urn:adam:file:file-hash"
                        :commit "feature-head" :branch "feature/a" :dirty? true}]
                      :observations
                      [{:id "observation-id" :producer "pi-observational-memory"
                        :adapter-version 1 :memory-id "aaaaaaaaaaaa"
                        :content "remember this" :timestamp "2026-01-01T00:00:00.000Z"
                        :relevance "high" :token-count 3 :recording-entry-id "memory-1"
                        :source-entry-ids ["entry-1"]
                        :file-ids ["urn:adam:file:file-hash"] :dropped? true}]
                      :reflections
                      [{:id "reflection-id" :producer "pi-observational-memory"
                        :adapter-version 1 :memory-id "bbbbbbbbbbbb"
                        :content "durable decision" :token-count 2
                        :recording-entry-id "memory-2"
                        :supporting-observation-ids ["aaaaaaaaaaaa"]}]}]
      (-> (knowledge-store/index-file-evidence! replica projection)
          (.then
           (fn [_]
             (let [queries (mapv :query @calls)
                   evidence-call (first (filter #(re-find #"UNWIND \$evidence" (:query %)) @calls))
                   observation-call (first (filter #(re-find #"SOURCED_FROM" (:query %)) @calls))
                   reflection-call (first (filter #(re-find #"SUPPORTED_BY" (:query %)) @calls))
                   ^js evidence (first (array-seq (aget (:params evidence-call) "evidence")))
                   ^js observation (first (array-seq (aget (:params observation-call) "observations")))
                   ^js reflection (first (array-seq (aget (:params reflection-call) "reflections")))]
               (is (some #(re-find #"DELETE touch" %) queries))
               (is (some #(re-find #"WORKED_ON" %) queries))
               (is (some #(re-find #"worked\.root = \$root" %) queries))
               (is (some #(re-find #"s\.codeMemoryVersion = \$extractorVersion" %) queries))
               (is (not-any? #(re-find #"\(u\)-\[:OWNS\]->\(repository\)" %) queries))
               (is (some #(re-find #"AdamCodeFile" %) queries))
               (is (some #(re-find #"DETACH DELETE memory" %) queries))
               (is (= "feature-head" (.-commit evidence)))
               (is (= "feature/a" (.-branch evidence)))
               (is (= true (.-dirty evidence)))
               (is (= "aaaaaaaaaaaa" (.-memoryId observation)))
               (is (= true (.-dropped observation)))
               (is (= "memory-1" (.-recordingEntryId observation)))
               (is (= "bbbbbbbbbbbb" (.-memoryId reflection)))
               (is (= 1 @closes))
               (done))))
          (.catch
           (fn [error]
             (is false (.-stack error))
             (done)))))))

(deftest clears-stale-file-evidence-and-marks-the-session-version
  (async done
    (let [{:keys [driver calls closes]} (recording-driver)
          replica (neo4j/replica-with-driver driver "neo4j")]
      (-> (knowledge-store/clear-file-evidence!
           replica "urn:adam:session:user-1:session-1" 3)
          (.then
           (fn [_]
             (let [queries (mapv :query (filter :query @calls))]
               (is (some #(re-find #"HAS_MEMORY" %) queries))
               (is (some #(re-find #"TOUCHES" %) queries))
               (is (some #(re-find #"DELETE worked" %) queries))
               (is (some #(re-find #"s.codeMemoryVersion = \$extractorVersion" %) queries))
               (is (= 1 @closes))
               (done))))
          (.catch
           (fn [error]
             (is false (.-stack error))
             (done)))))))

(deftest persists-code-memory-rebuild-version-after-removing-legacy-ownership
  (async done
    (let [{:keys [driver calls closes]} (recording-driver)
          replica (neo4j/replica-with-driver driver "neo4j")]
      (-> (knowledge-store/code-memory-version!
           replica "urn:adam:user:user-1")
          (.then
           (fn [version]
             (is (nil? version))
             (knowledge-store/complete-code-memory-rebuild!
              replica "urn:adam:user:user-1" 3)))
          (.then
           (fn [_]
             (let [queries (mapv :query (filter :query @calls))]
               (is (some #(re-find #"min\(coalesce\(s.codeMemoryVersion" %) queries))
               (is (some #(re-find #"codeMemoryVersion" %) queries))
               (is (some #(re-find #"legacyOwnership:OWNS" %) queries))
               (is (some #(re-find #"DETACH DELETE repository" %) queries))
               (is (some #(re-find #"u.codeMemoryVersion = \$version" %) queries))
               (is (= 2 @closes))
               (done))))
          (.catch
           (fn [error]
             (is false (.-stack error))
             (done)))))))

(deftest queries-shared-code-identity-through-requesting-user-sessions
  (async done
    (let [{:keys [driver calls closes]} (recording-driver)
          replica (neo4j/replica-with-driver driver "neo4j")]
      (-> (knowledge-store/query-file-memory!
           replica
           "urn:adam:user:user-1"
           "urn:adam:repository:git:hash"
           "src/a.cljs"
           10)
          (.then
           (fn [_]
             (let [query (:query (first (filter :query @calls)))]
               (is (re-find #"AdamUser.*OWNS.*AdamSession.*HAS_MEMORY" query))
               (is (not (re-find #"OWNS.*AdamRepository" query)))
               (is (re-find #"AdamRepository.*CONTAINS.*AdamCodeFile" query))
               (is (= 1 @closes))
               (done))))
          (.catch
           (fn [error]
             (is false (.-stack error))
             (done)))))))
