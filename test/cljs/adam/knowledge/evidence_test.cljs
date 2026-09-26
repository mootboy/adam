(ns adam.knowledge.evidence-test
  (:require [adam.knowledge.evidence :as evidence]
            [cljs.test :refer [deftest is testing]]))

(def user-uuid "00000000-0000-4000-8000-000000000001")

(def repository
  (evidence/build-repository
   {:user-uuid user-uuid
    :root "/work/repo"
    :remote "git@github.com:AloiAI/adam.git"
    :commit "main-head"
    :branch "main"
    :dirty? false}))

(defn- stored [entry]
  {:entry-id (:id entry) :raw-json (js/JSON.stringify (clj->js entry))})

(deftest normalizes-canonical-repository-identity-without-credentials-or-user-scope
  (let [ssh (evidence/build-repository
             {:user-uuid user-uuid :root "/one"
              :remote "git@github.com:AloiAI/adam.git"
              :commit "a" :dirty? false})
        other-user (evidence/build-repository
                    {:user-uuid "other-user" :root "/two"
                     :remote "https://github.com/AloiAI/adam.git"
                     :commit "b" :dirty? false})
        credentialed (evidence/build-repository
                      {:user-uuid user-uuid :root "/three"
                       :remote "https://secret@example.com/AloiAI/adam.git"
                       :commit "c" :dirty? false})]
    (is (= "github.com/AloiAI/adam" (:normalized-remote ssh)))
    (is (= (:id repository) (:id ssh) (:id other-user)))
    (is (= (:id (evidence/resolve-origin-file ssh "src/a.cljs"))
           (:id (evidence/resolve-origin-file other-user "src/a.cljs"))))
    (is (string? (:id ssh)))
    (is (= "example.com/AloiAI/adam" (:normalized-remote credentialed)))
    (is (not (re-find #"secret" (pr-str credentialed))))))

(deftest keeps-originless-repositories-user-scoped-and-nonportable
  (let [first-user (evidence/build-repository
                    {:user-uuid "user-1" :root "/work/repo"
                     :commit "a" :dirty? false})
        second-user (evidence/build-repository
                     {:user-uuid "user-2" :root "/work/repo"
                      :commit "a" :dirty? false})]
    (is (not= (:id first-user) (:id second-user)))
    (is (re-find #"^urn:adam:repository:local:user-1:" (:id first-user)))
    (is (nil? (:normalized-remote first-user)))))

(deftest canonicalizes-worktree-paths-and-retains-the-actual-revision
  (let [with-worktree
        (evidence/build-repository
         {:user-uuid user-uuid
          :root "/work/repo"
          :remote "git@github.com:AloiAI/adam.git"
          :commit "main-head"
          :branch "main"
          :dirty? false
          :worktrees [{:root "/work/repo" :commit "main-head" :branch "main" :dirty? false}
                      {:root "/work/trees/feature" :commit "feature-head"
                       :branch "feature/a" :dirty? true}]})
        projection
        (evidence/extract-projection
         {:user-uuid user-uuid
          :pi-session-id "session-1"
          :session-id "urn:adam:session:user:session-1"
          :cwd "/work/repo"
          :repository with-worktree
          :entries
          [(stored {:type "message" :id "assistant-1" :parentId nil
                    :message {:role "assistant"
                              :content [{:type "toolCall" :id "call-1" :name "edit"
                                         :arguments {:path "/work/trees/feature/src/a.cljs"}}]}})
           (stored {:type "message" :id "result-1" :parentId "assistant-1"
                    :message {:role "toolResult" :toolCallId "call-1"}})]})]
    (is (= 3 (:extractor-version projection)))
    (is (= ["src/a.cljs"] (mapv :relative-path (:files projection))))
    (is (= (:id (evidence/resolve-repository-file with-worktree "/work/repo" "src/a.cljs"))
           (get-in projection [:files 0 :id])))
    (is (= [{:entry-id "assistant-1"
             :file-id (get-in projection [:files 0 :id])
             :commit "feature-head" :branch "feature/a" :dirty? true}
            {:entry-id "result-1"
             :file-id (get-in projection [:files 0 :id])
             :commit "feature-head" :branch "feature/a" :dirty? true}]
           (:entry-file-evidence projection)))))

(deftest indexes-only-explicit-file-tools-on-the-selected-branch
  (let [entries
        [(stored {:type "message" :id "root" :parentId nil
                  :message {:role "user" :content "start"}})
         (stored {:type "message" :id "assistant-a" :parentId "root"
                  :message {:role "assistant"
                            :content [{:type "toolCall" :id "call-a" :name "read"
                                       :arguments {:path "src/a.cljs"}}]}})
         (stored {:type "message" :id "leaf-a" :parentId "assistant-a"
                  :message {:role "assistant" :content []}})
         (stored {:type "message" :id "assistant-b" :parentId "root"
                  :message {:role "assistant"
                            :content [{:type "text" :text "read src/prose.cljs"}
                                      {:type "toolCall" :id "call-shell" :name "bash"
                                       :arguments {:path "src/shell.cljs"}}
                                      {:type "toolCall" :id "call-out" :name "write"
                                       :arguments {:path "../outside.cljs"}}
                                      {:type "toolCall" :id "call-b" :name "read"
                                       :arguments {:path "src/b.cljs"}}]}})
         (stored {:type "message" :id "leaf-b" :parentId "assistant-b"
                  :message {:role "toolResult" :toolCallId "call-b"}})]
        projection (evidence/extract-projection
                    {:user-uuid user-uuid :pi-session-id "session-1"
                     :session-id "session-id" :cwd "/work/repo"
                     :repository repository :entries entries
                     :current-leaf-id "leaf-b"})]
    (is (= ["src/b.cljs"] (mapv :relative-path (:files projection))))
    (is (= ["assistant-b" "leaf-b"]
           (mapv :entry-id (:entry-file-evidence projection))))
    (is (= ["../outside.cljs" "src/b.cljs"]
           (evidence/explicit-file-tool-paths entries "leaf-b")))))


(deftest links-provider-neutral-memories-through-source-file-evidence
  (let [memory-projection
        {:observations [{:producer "pi-observational-memory" :adapter-version 1
                         :memory-id "aaaaaaaaaaaa" :content "Uses the source result"
                         :timestamp "2026-01-01T00:00:00.000Z" :relevance "high"
                         :token-count 4 :recording-entry-id "memory-1"
                         :source-entry-ids ["result-1"] :dropped? false}]
         :reflections [{:producer "pi-observational-memory" :adapter-version 1
                        :memory-id "bbbbbbbbbbbb" :content "Keep this decision"
                        :token-count 3 :recording-entry-id "memory-2"
                        :supporting-observation-ids ["aaaaaaaaaaaa"]}]
         :diagnostics [{:producer "pi-observational-memory"
                        :entry-id "bad-1" :reason :malformed-entry}]}
        projection
        (evidence/extract-projection
         {:user-uuid user-uuid :pi-session-id "session-1"
          :session-id "session-id" :cwd "/work/repo"
          :repository repository
          :entries
          [(stored {:type "message" :id "assistant-1" :parentId nil
                    :message {:role "assistant"
                              :content [{:type "toolCall" :id "call-1" :name "read"
                                         :arguments {:path "src/a.cljs"}}]}})
           (stored {:type "message" :id "result-1" :parentId "assistant-1"
                    :message {:role "toolResult" :toolCallId "call-1"}})
           (stored {:type "custom" :id "memory-1" :parentId "result-1"})
           (stored {:type "custom" :id "memory-2" :parentId "memory-1"})]
          :memory-projection memory-projection})]
    (is (= "urn:adam:observation:00000000-0000-4000-8000-000000000001:session-1:aaaaaaaaaaaa"
           (get-in projection [:observations 0 :id])))
    (is (= [(get-in projection [:files 0 :id])]
           (get-in projection [:observations 0 :file-ids])))
    (is (= "urn:adam:reflection:00000000-0000-4000-8000-000000000001:session-1:bbbbbbbbbbbb"
           (get-in projection [:reflections 0 :id])))
    (is (= (:diagnostics memory-projection) (:memory-diagnostics projection)))))
