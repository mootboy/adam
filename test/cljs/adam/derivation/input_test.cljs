(ns adam.derivation.input-test
  (:require [adam.derivation.config :as config]
            [adam.derivation.core :as core]
            [adam.derivation.store :as store]
            [adam.derivation.publication-test :as publication]
            [adam.derivation.store-test :refer [reader]]
            [clojure.string :as string]
            ["node:fs" :refer [readFileSync]]
            [cljs.test :refer-macros [deftest is async]]))

(defn- fixture
  ([memory-rows] (fixture memory-rows {}))
  ([memory-rows {:keys [prior-rows dependency-rows activity-rows duplicate-rows activity-error activity-callback-error]}]
   (let [calls (atom []) closes (atom 0) session-options (atom nil) activity-row-reads (atom 0)
         record (fn [row] #js {:get (fn [key] (get row key))})
         tx #js {:run (fn [query params]
                        (swap! calls conj {:query query :params params})
                        (if (re-find #"AS activityEntry" query)
                          #js {:subscribe (fn [observer]
                                            (when activity-callback-error
                                              ((aget observer "onNext") #js {:get (fn [_] (swap! activity-row-reads inc)
                                                                                  (throw (js/Error. "row-read-failed")))}))
                                            (doseq [row activity-rows
                                                    :let [source (get row "activitySession")]
                                                    :when (= (aget params "sessionId") (aget source "id"))
                                                    entry (array-seq (aget source "entries"))]
                                              ((aget observer "onNext") #js {:get (fn [_] (swap! activity-row-reads inc) entry)}))
                                            (if activity-error ((aget observer "onError") activity-error)
                                                ((aget observer "onCompleted") #js {})))}
                          (js/Promise.resolve
                            #js {:records (to-array
                                            (map record
                                              (cond
                                                (re-find #"RETURN u.id AS userId" query) [{"userId" reader}]
                                                (re-find #"AS inputMemory" query) memory-rows
                                                (re-find #"AS activitySession" query)
                                                (map (fn [row] (let [metadata (js/Object.assign #js {} (get row "activitySession"))]
                                                                 (js-delete metadata "entries") {"activitySession" metadata})) activity-rows)
                                                (re-find #"AS duplicateClaudeRequest" query) duplicate-rows
                                                (re-find #"AS parents" query) prior-rows
                                                (re-find #"AS ownerIds" query) dependency-rows
                                                :else [])))})))}]
     {:driver #js {:session (fn [options] (reset! session-options options)
                            #js {:executeRead (fn [work] (work tx))
                                 :close (fn [] (swap! closes inc) (js/Promise.resolve))})}
      :calls calls :closes closes :session-options session-options :activity-row-reads activity-row-reads})))

(deftest source-driver-streams-are-consumed-without-buffering-and-close-on-error
  (async done
    (let [sid "urn:adam:session:reader:pi:s" parent "urn:adam:session:reader:pi:p"
          entry #js {:entryId "u" :streamId "main" :rawJson "{\"id\":\"u\",\"type\":\"message\",\"message\":{\"role\":\"user\",\"content\":\"Hello\"}}"}
          source (fn [id host parents] {"activitySession" #js {:id id :sourceKind "pi" :sourceSessionId host
                                                               :ownerIds #js [reader] :parentIds (to-array parents) :entries #js [entry]}})
          row {"ownerIds" [reader] "sessionIds" [sid] "sourceKind" "pi" "sourceSessionId" "s" "kind" "observation"
               "sourceEntries" #js [#js {:streamId "main" :entryId "u"}]
               "inputMemory" #js {:id "m" :producer "synthetic" :content "Preference"}}
          attempt (fn [failure?]
                    (let [{:keys [driver calls closes session-options activity-row-reads]} (fixture [row] {:activity-rows [(source sid "s" [parent]) (source parent "p" [])]
                                                                                       :activity-error (when (= :sdk failure?) (js/Error. "stream-unavailable"))
                                                                                       :activity-callback-error (= :callback failure?)})]
                      (-> (store/load-input! (store/with-driver driver "neo4j" reader) {:name "preferences"})
                          (.then (fn [snapshot]
                                   (is (not failure?))
                                   (is (= [{:session-id parent :stream-id "main" :entry-id "u"}] (get-in snapshot [:memories 0 :source-occasions])))
                                   (is (= 2 (count (filter #(re-find #"AS activityEntry" (:query %)) @calls)))))
                                 (fn [error] (is failure?) (is (= (if (= :callback failure?) "row-read-failed" "stream-unavailable") (.-message error)))))
                          (.then (fn [_]
                                   (is (= 1 @closes))
                                   (is (= 100 (aget @session-options "fetchSize")))
                                   (when (= :callback failure?) (is (= 1 @activity-row-reads)))
                                   (doseq [{:keys [query params]} @calls :when (re-find #"AS activityEntry" query)]
                                     (is (= reader (aget params "ownerId")))
                                     (is (re-find #"other <> owner" query)))
                                   (is (every? #(not (re-find #"rawJson" (:query %)))
                                               (filter #(re-find #"AS activitySession" (:query %)) @calls))))))))]
      (-> (js/Promise.all #js [(attempt false) (attempt :sdk) (attempt :callback)])
          (.catch (fn [error] (is false (.-stack error)))) (.finally done)))))

(deftest authorized-input-snapshots-are-complete-and-closed
  (async done
    (let [row {"ownerIds" [reader] "sessionIds" ["urn:adam:session:reader:pi:s"]
               "sourceKind" "pi" "sourceSessionId" "s"
               "inputMemory" #js {:id "memory-a" :producer "synthetic" :content "Prefer simple boundaries"}
               "supportRecords" [] "kind" "observation" "sourceEntries" #js [#js {:streamId "main" :entryId "entry-a"}]
               "repositoryOrigins" ["example.com/repo"] "repositoryIds" ["repo-id"]
               "linkedPaths" ["src/a.cljs"]}
          cases [{:rows [row] :ids ["memory-a"]}
                 {:rows [] :ids []}
                 {:rows [(assoc row "inputMemory" #js {:id "memory-a" :producer "synthetic" :content "Dropped support" :dropped true})]
                  :ids ["memory-a"] :dropped? true}
                 {:rows [(assoc row "kind" "reflection" "supportRecords" [#js {:id "actual-support-id" :memoryId "support" :producer "synthetic" :sessionIds #js ["urn:adam:session:reader:pi:s"]}] "inputMemory" #js {:id "reflection-a" :producer "synthetic" :content "Scoped synthesis" :supportingObservationIds #js ["support"]})]
                  :ids ["reflection-a"] :supports ["actual-support-id"]}
                 {:rows [(assoc row "kind" "reflection" "supportRecords" [#js {:id "actual-support-id" :memoryId "support" :producer "synthetic" :sessionIds #js ["urn:adam:session:reader:pi:s"]}]
                                "inputMemory" #js {:id "reflection-a" :producer "synthetic" :content "Scoped synthesis" :dropped true :supportingObservationIds #js ["support"]})]
                  :ids ["reflection-a"] :supports ["actual-support-id"] :ignore-drop? true}
                 {:rows [(assoc row "kind" "reflection")] :error :unresolved-evidence}
                 {:rows [(assoc row "ownerIds" ["not-a-user-urn"])] :error :unresolved-evidence}
                 {:rows [(assoc row "sourceSessionId" "different")] :error :unresolved-evidence}
                 {:rows [(assoc row "kind" nil)] :error :unresolved-evidence}
                 {:rows [(assoc row "ownerIds" [reader "other"])] :error :unresolved-evidence}]
          attempt (fn [{:keys [rows ids error supports dropped? ignore-drop?]}]
                    (let [{:keys [driver closes]} (fixture rows)]
                      (-> (store/load-input! (store/with-driver driver "neo4j" reader) {:name "preferences"})
                          (.then (fn [snapshot]
                                   (is (nil? error))
                                   (is (= reader (:reader-id snapshot)))
                                   (is (= ids (mapv :id (:memories snapshot))))
                                   (when (seq ids)
                                     (is (= [{:stream-id "main" :entry-id "entry-a"}]
                                            (get-in snapshot [:memories 0 :source-entries])))
                                     (is (= [] (get-in snapshot [:memories 0 :source-occasions]))))
                                   (when supports (is (= supports (get-in snapshot [:memories 0 :supporting-memory-ids]))))
                                   (when dropped? (is (get-in snapshot [:memories 0 :dropped?])))
                                   (when ignore-drop? (is (false? (get-in snapshot [:memories 0 :dropped?]))))))
                          (.catch (fn [failure] (is (= error (config/error-code failure)))))
                          (.finally (fn [] (is (= 1 @closes)))))))]
      (-> (js/Promise.all (to-array (map attempt cases)))
          (.then (fn [_] (done)))
          (.catch (fn [error] (is false (.-stack error)) (done)))))))

(deftest invalid-input-selectors-fail-before-driver-access
  (async done
    (let [opens (atom 0) db (store/with-driver #js {:session (fn [_] (swap! opens inc))} "neo4j" reader)]
      (-> (store/load-input! db {:name "preferences" :repository "shorthand"})
          (.then (fn [_] (is false "invalid selector must fail")))
          (.catch (fn [error] (is (= :invalid-selector (config/error-code error))) (is (zero? @opens))))
          (.finally done)))))

(deftest configured-refresh-wires-directly-to-the-graph-loader-for-every-selector
  (async done
    (let [recipes (config/parse-config "derivations:\n  preferences:\n    version: 1\n    guidance: Preserve scope.\n")
          row {"ownerIds" [reader] "sessionIds" ["urn:adam:session:reader:pi:s"]
               "sourceKind" "pi" "sourceSessionId" "s" "kind" "observation"
               "inputMemory" #js {:id "memory-a" :producer "synthetic" :content "Prefer simple boundaries"}
               "sourceEntries" [] "repositoryOrigins" ["example.com/repo"] "linkedPaths" ["src/a.cljs"]}
          requests [{:name "preferences"} {:name "preferences" :repository "git@example.com:repo.git"}
                    {:name "preferences" :session "urn:adam:session:reader:pi:s"}
                    {:name "preferences" :language "cljs"}]
          attempt (fn [request]
                    (let [{:keys [driver]} (fixture [row])
                          db (store/with-driver driver "neo4j" reader)
                          deps {:load-input! (fn [request]
                                              (-> (store/load-input! db request)
                                                  (.then #(assoc % :model {:provider "fake" :id "model" :context-window 20000 :max-output-tokens 8192}))))
                                :measure-input (constantly 100)
                                :execute! (fn [request]
                                            (is (= ["memory-a"] (vec (aget (js/JSON.parse (:payload request)) "candidate-memory-ids"))))
                                            {:finish-reason :stop :conclusions [] :links []})}]
                      (-> (core/refresh-configured! deps recipes request)
                          (.then (fn [outcomes] (is (= :generated (:status (first outcomes)))))))))]
      (-> (js/Promise.all (to-array (map attempt requests)))
          (.then (fn [_] (done)))
          (.catch (fn [error] (is false (.-stack error)) (done)))))))

(deftest revoked-prior-dependencies-withhold-all-comparison-text
  (async done
    (-> (publication/generated-run)
        (.then (fn [run]
                 (let [{:keys [driver]} (fixture []
                                         {:prior-rows [(publication/run-row run)]
                                          :dependency-rows [{"memoryId" "memory-a" "ownerIds" ["urn:adam:user:other"]
                                                             "sessionIds" ["other-session"] "kind" "observation"
                                                             "producer" "synthetic" "content" "PRIVATE"
                                                             "permitted" false "linked" true}]})]
                   (store/load-input! (store/with-driver driver "neo4j" reader) {:name "preferences"}))))
        (.then (fn [snapshot]
                 (is (= {:id "unavailable"} (:comparison snapshot)))
                 (is (= [] (:memories snapshot)))
                 (is (not (re-find #"PRIVATE" (pr-str snapshot))))))
        (.catch (fn [error] (is false (.-stack error))))
        (.finally done))))

(deftest malformed-source-outcomes-retain-content-free-diagnostic-counts
  (async done
    (let [{:keys [driver]} (fixture (conj (vec (repeat 3 {"ownerIds" [reader "other"] "sessionIds" [] "inputMemory" nil}))
                                              {"ownerIds" [reader] "sessionIds" ["urn:adam:session:reader:pi:s"]
                                               "sourceKind" "pi" "sourceSessionId" "s" "kind" "observation"
                                               "inputMemory" #js {:id "healthy" :producer "synthetic" :content "Healthy evidence"}}))
          deps {:load-input! #(store/load-input! (store/with-driver driver "neo4j" reader) %)
                :execute! (fn [_] (is false "malformed evidence must never execute"))}
          recipes (config/parse-config "derivations:\n  preferences:\n    version: 1\n    guidance: Preserve scope.\n")]
      (-> (core/refresh-configured! deps recipes {:name "preferences"})
          (.then (fn [outcomes]
                   (is (= [{:name "preferences" :status :failed :code :unresolved-evidence :invalid-source-count 3}] outcomes))))
          (.catch (fn [error] (is false (.-stack error))))
          (.finally done)))))

(deftest native-source-occasion-anchors-share-the-original-user-activity
  (async done
    (let [sid "urn:adam:session:reader:pi:s"
          entry (fn [id parent message] {:entryId id :streamId "main"
                                        :rawJson (js/JSON.stringify (clj->js {:id id :type "message" :parentId parent :message message}))})
          u (entry "user" nil {:role "user" :content "Use specs."})
          a (entry "call" "user" {:role "assistant" :content [{:type "toolCall" :id "t" :name "read" :arguments {:path "a.cljs"}}]})
          r (entry "result" "call" {:role "toolResult" :toolCallId "t" :content []})
          summary {:entryId "summary" :streamId "main" :rawJson "{\"id\":\"summary\",\"type\":\"compaction\",\"parentId\":\"result\"}"}
          origin (fn [id host entries parent-ids] {"activitySession" (clj->js {:id id :sourceKind "pi" :sourceSessionId host
                                              :ownerIds [reader] :entries entries :parentIds parent-ids})})
          row (fn [id cites] {"ownerIds" [reader] "sessionIds" [sid] "sourceKind" "pi" "sourceSessionId" "s" "kind" "observation"
                             "sourceEntries" (clj->js (mapv #(hash-map :streamId "main" :entryId %) cites))
                             "inputMemory" #js {:id id :producer "synthetic" :content "Prefer specs"}})
          anchor {:session-id sid :stream-id "main" :entry-id "user"}
          cases [{:label "call and result share one user request" :entries [u a r] :cites ["call" "result"] :expected [anchor]}
                 {:label "citation strings alone are not verified" :entries [] :cites ["user"] :expected []}
                 {:label "summaries are not confirmations" :entries [u a r summary] :cites ["summary"] :expected []}
                 {:label "unmarked Pi relay is indistinguishable from a native request"
                  :entries [(entry "user" nil {:role "user" :content "Relayed message"})]
                  :cites ["user"] :expected [anchor]}
                 {:label "deep ignored payload does not destroy a verified request"
                  :entries [{:entryId "user" :streamId "main"
                             :rawJson (str "{\"id\":\"user\",\"type\":\"message\",\"parentId\":null,\"message\":{\"role\":\"user\",\"content\":\"Request\"},\"ignored\":"
                                           (apply str (repeat 20000 "[")) "0" (apply str (repeat 20000 "]")) "}")}]
                  :cites ["user"] :expected [anchor]}
                 {:label "image-only native request" :entries [(entry "user" nil {:role "user" :content [{:type "image" :data "synthetic"}]})]
                  :cites ["user"] :expected [anchor]}
                 {:label "empty text blocks do not confirm" :entries [(entry "user" nil {:role "user" :content [{:type "text" :text ""}]})]
                  :cites ["user"] :expected []}
                 {:label "cycle cannot anchor" :entries [(entry "cycle" "cycle" {:role "assistant" :content []})] :cites ["cycle"] :expected []}
                 {:label "new user activity is distinct" :entries [u (entry "second" "user" {:role "user" :content "Repeat correction."})]
                  :cites ["user" "second"] :expected [(assoc anchor :entry-id "second") anchor]}]
          attempt (fn [{:keys [label entries cites expected]}]
                    (let [{:keys [driver]} (fixture [(row "memory-a" cites)]
                                                    {:activity-rows [(origin sid "s" entries [])]})]
                      (-> (store/load-input! (store/with-driver driver "neo4j" reader) {:name "preferences"})
                          (.then (fn [snapshot] (is (= expected (get-in snapshot [:memories 0 :source-occasions])) label))))))]
      (-> (js/Promise.all (to-array (map attempt cases)))
          (.then (fn [_] (done)))
          (.catch (fn [error] (is false (.-stack error)) (done)))))))

(deftest verified-fork-origins-do-not-manufacture-new-occasions
  (async done
    (let [sid "urn:adam:session:reader:pi:fork" parent "urn:adam:session:reader:pi:origin"
          record {:entryId "user" :streamId "main" :rawJson "{\"id\":\"user\",\"type\":\"message\",\"parentId\":null,\"message\":{\"role\":\"user\",\"content\":\"Use specs.\"}}"}
          source (fn [id host parents entries extra] {"activitySession" (clj->js (merge {:id id :sourceKind "pi" :sourceSessionId host
                                                                :ownerIds [reader] :parentIds parents :entries entries} extra))})
          row {"ownerIds" [reader] "sessionIds" [sid] "sourceKind" "pi" "sourceSessionId" "fork" "kind" "observation"
               "sourceEntries" #js [#js {:streamId "main" :entryId "user"}] "inputMemory" #js {:id "memory-a" :producer "synthetic" :content "Use specs"}}
          cases [{:parents [(source parent "origin" [] [record] {})] :expected [{:session-id parent :stream-id "main" :entry-id "user"}]}
                 {:parents [] :expected []}
                 {:parents [(source parent "origin" [] [(assoc record :rawJson (str (:rawJson record) " "))] {})] :expected []}
                 {:parents [(source parent "origin" [] [] {})] :expected [{:session-id sid :stream-id "main" :entry-id "user"}]}
                 {:parents [(source parent "origin" [] [record] {:ownerIds ["urn:adam:user:other"]})] :expected []}
                 {:parents [(source parent "origin" [sid] [record] {})] :expected []}]
          attempt (fn [{:keys [parents expected]}]
                    (let [{:keys [driver]} (fixture [row] {:activity-rows (into [(source sid "fork" [parent] [record] {})] parents)})]
                      (-> (store/load-input! (store/with-driver driver "neo4j" reader) {:name "preferences"})
                          (.then (fn [snapshot] (is (= expected (get-in snapshot [:memories 0 :source-occasions]))))))))]
      (-> (js/Promise.all (to-array (map attempt cases)))
          (.then (fn [_] (done))) (.catch (fn [error] (is false (.-stack error)) (done)))))))

(deftest claude-native-activity-anchors-parallel-tools-compaction-and-children
  (async done
    (let [sid "urn:adam:session:reader:claude-code:s"
          entry (fn [id parent type content extra]
                  {:entryId id :streamId (or (:stream extra) "main")
                   :rawJson (js/JSON.stringify (clj->js (merge {:uuid id :parentUuid parent :type type :sessionId "s"
                                                               :message {:role type :content content}}
                                                              (dissoc extra :stream))))})
          u (entry "user" nil "user" "Use specs." {})
          call (entry "call" "user" "assistant" [{:type "tool_use" :id "t" :name "Read" :input {:file_path "a.cljs"}}] {:requestId "request"})
          fragment (entry "fragment" nil "assistant" [{:type "text" :text "Response fragment"}] {:requestId "request"})
          result (entry "result" "user" "user" [{:type "tool_result" :tool_use_id "t" :content "Read done"}] {:sourceToolAssistantUUID "call"})
          agent (entry "agent-call" "user" "assistant" [{:type "tool_use" :id "agent-tool" :name "Agent" :input {}}] {})
          handback (entry "handback" "agent-call" "user" [{:type "tool_result" :tool_use_id "agent-tool" :content "Assistant handback"}]
                          {:sourceToolAssistantUUID "agent-call" :toolUseResult {:agentId "child"}})
          child (entry "child-call" nil "assistant" [{:type "tool_use" :id "c" :name "Read" :input {:file_path "b.cljs"}}]
                       {:stream "agent:child" :agentId "child"})
          anchor {:session-id sid :stream-id "main" :entry-id "user"}
          cases (into (mapv (fn [line]
                                   (let [record (js->clj (js/JSON.parse line) :keywordize-keys true)
                                         id (:uuid record)]
                                     {:records [{:entryId id :streamId "main" :rawJson line}]
                                      :cites [["main" id]] :expected (if (= id "notification") [] [(assoc anchor :entry-id id)])}))
                                 (string/split-lines (readFileSync "test/fixtures/derivation/claude-interactive-origins.jsonl" "utf8")))
                       [{:records (into [u] (map (fn [i] (entry (str "long-" i) (if (zero? i) "user" (str "long-" (dec i)))
                                                               "assistant" "Continuation" {})) (range 15000)))
                  :cites [["main" "long-14999"]] :expected [anchor]}
                 {:records [u call result fragment] :cites [["main" "call"] ["main" "result"] ["main" "fragment"]] :expected [anchor]}
                 {:records [u call (entry "dup" "user" "assistant" [{:type "tool_use" :id "t" :name "Read"}] {}) result]
                  :cites [["main" "result"]] :expected []}
                 {:records [u call (entry "result" "user" "user" [{:type "tool_result" :tool_use_id "t"}] {:sourceToolAssistantUUID "wrong"})]
                  :cites [["main" "result"]] :expected []}
                 {:records [u (entry "summary" "user" "user" "Use specs many times." {:isCompactSummary true})]
                  :cites [["main" "summary"]] :expected []}
                 {:records [u call
                            {:entryId "boundary" :streamId "main"
                             :rawJson "{\"uuid\":\"boundary\",\"sessionId\":\"s\",\"type\":\"system\",\"subtype\":\"compact_boundary\",\"parentUuid\":null,\"logicalParentUuid\":\"call\"}"}
                            (entry "summary" "boundary" "user" "Generated continuation context." {:isCompactSummary true})
                            (entry "continued" "summary" "assistant" [{:type "text" :text "Continue task"}] {})]
                  :cites [["main" "continued"]] :expected [anchor]}
                 {:records [u agent handback child] :cites [["main" "handback"] ["agent:child" "child-call"]] :expected [anchor]}
                 {:records [u child] :cites [["agent:child" "child-call"]] :expected []}
                 {:records [u call result] :cites [["main" "result"]] :duplicates [{"duplicateClaudeRequest" #js {:ownerId reader :entryId "user" :streamId "main"}}] :expected []}
                 {:records [(entry "injected" nil "user" "<task-notification>Complete</task-notification>" {})]
                  :cites [["main" "injected"]] :expected []}
                 {:records [(entry "injected" nil "user" "[Request interrupted by user]" {})]
                  :cites [["main" "injected"]] :expected []}
                 {:records [(entry "injected" nil "user" "<command-name>/compact</command-name>" {})]
                  :cites [["main" "injected"]] :expected []}
                 {:records [(entry "command" nil "user" "<command-message>custom</command-message>\n<command-name>/custom</command-name>" {})]
                  :cites [["main" "command"]] :expected []}
                 {:records [(entry "command" nil "user" "<command-message>custom</command-message>\n<command-name>/custom</command-name>"
                                    {:promptSource "sdk" :turnOrigin "sdk" :userType "external"})]
                  :cites [["main" "command"]] :expected [(assoc anchor :entry-id "command")]}
                 {:records [(entry "command" nil "user" "<command-name>/custom</command-name>"
                                    {:promptSource "sdk" :turnOrigin "sdk" :userType "external"})]
                  :cites [["main" "command"]] :expected [(assoc anchor :entry-id "command")]}
                 {:records [(entry "injected" nil "user" "Done."
                                    {:promptSource "task-notification" :turnOrigin "task" :userType "internal"})]
                  :cites [["main" "injected"]] :expected []}
                 {:records [u (entry "agent-call" "user" "assistant"
                                      [{:type "tool_use" :id "agent-tool" :name "Read"}
                                       {:type "tool_use" :id "different" :name "Agent"}] {}) handback child]
                  :cites [["agent:child" "child-call"]] :expected []}
                 {:records [u (entry "new" nil "user" "Correction after compaction." {})]
                  :cites [["main" "new"]] :expected [(assoc anchor :entry-id "new")]}
                 {:records [(entry "echo" nil "user" "<command-name>/mcp</command-name>" {:userType "external"})]
                  :cites [["main" "echo"]] :expected []}
                 {:records [(entry "echo" nil "user" "<command-message>/mcp</command-message>" {:userType "external"})]
                  :cites [["main" "echo"]] :expected []}
                 {:records [(entry "output" nil "user" "<bash-stdout>Result</bash-stdout>" {:turnOrigin "human" :userType "external"})]
                  :cites [["main" "output"]] :expected []}
                 {:records [(entry "output" nil "user" "<bash-stderr>Error</bash-stderr>" {:turnOrigin "human" :userType "external"})]
                  :cites [["main" "output"]] :expected []}
                 {:records [(entry "shell" nil "user" "<bash-input>pwd</bash-input>" {:turnOrigin "human" :userType "external"})]
                  :cites [["main" "shell"]] :expected [(assoc anchor :entry-id "shell")]}
                 {:records [(entry "unknown" nil "user" "Hello" {:turnOrigin "future-unknown" :userType "external"})]
                  :cites [["main" "unknown"]] :expected []}
                 {:records [(entry "unknown" nil "user" "Hello" {:turnOrigin "human" :promptSource "future-unknown" :userType "external"})]
                  :cites [["main" "unknown"]] :expected []}
                 {:records [(entry "command" nil "user" "<command-message>custom</command-message>\n<command-name>/custom</command-name>"
                                    {:turnOrigin "human" :userType "external"})]
                  :cites [["main" "command"]] :expected [(assoc anchor :entry-id "command")]}])
          attempt (fn [{:keys [records cites expected duplicates]}]
                    (let [row {"ownerIds" [reader] "sessionIds" [sid] "sourceKind" "claude-code" "sourceSessionId" "s" "kind" "observation"
                               "sourceEntries" (clj->js (mapv (fn [[stream id]] {:streamId stream :entryId id}) cites))
                               "inputMemory" #js {:id "memory-a" :producer "synthetic" :content "Scoped preference"}}
                          {:keys [driver]} (fixture [row] {:duplicate-rows duplicates :activity-rows [{"activitySession" (clj->js {:id sid :sourceKind "claude-code" :sourceSessionId "s"
                                                                                                    :ownerIds [reader] :entries records :parentIds []})}]})]
                      (-> (store/load-input! (store/with-driver driver "neo4j" reader) {:name "preferences"})
                          (.then (fn [snapshot] (is (= expected (get-in snapshot [:memories 0 :source-occasions]))))))))]
      (-> (js/Promise.all (to-array (map attempt cases)))
          (.then (fn [_] (done))) (.catch (fn [error] (is false (.-stack error)) (done)))))))

(deftest actual-claude-fork-requests-cannot-confirm-the-same-event-twice
  (async done
    (let [lines (string/split-lines (readFileSync "test/fixtures/derivation/claude-fork-user-copies.jsonl" "utf8"))
          records (mapv #(js->clj (js/JSON.parse %) :keywordize-keys true) lines)
          host-id (:sessionId (last records))
          sid (str "urn:adam:session:reader:claude-code:" host-id)
          copies (filterv #(= host-id (:sessionId %)) records)
          entries (mapv (fn [record] {:entryId (:uuid record) :streamId "main" :rawJson (js/JSON.stringify (clj->js record))}) copies)
          duplicate-rows (mapv (fn [record] {"duplicateClaudeRequest" #js {:ownerId reader :streamId "main" :entryId (:uuid record)}}) copies)
          row {"ownerIds" [reader] "sessionIds" [sid] "sourceKind" "claude-code" "sourceSessionId" host-id "kind" "observation"
               "sourceEntries" (clj->js (mapv (fn [record] {:streamId "main" :entryId (:uuid record)}) copies))
               "inputMemory" #js {:id "memory-a" :producer "synthetic" :content "Copied request"}}
          {:keys [driver]} (fixture [row] {:duplicate-rows duplicate-rows
                                          :activity-rows [{"activitySession" (clj->js {:id sid :sourceKind "claude-code" :sourceSessionId host-id
                                                                                     :ownerIds [reader] :entries entries :parentIds []})}]})]
      (is (= 2 (count copies)))
      (is (= 2 (count (set (map :sessionId records)))))
      (is (= 2 (count (set (map :uuid records)))))
      (-> (store/load-input! (store/with-driver driver "neo4j" reader) {:name "preferences"})
          (.then (fn [snapshot] (is (= [] (get-in snapshot [:memories 0 :source-occasions])))))
          (.catch (fn [error] (is false (.-stack error))))
          (.finally done)))))
