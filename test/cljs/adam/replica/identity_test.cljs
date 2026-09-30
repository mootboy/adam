(ns adam.replica.identity-test
  (:require [adam.replica.identity :as identity]
            [cljs.test :refer [deftest is]]))

(deftest resource-identities-are-user-session-and-source-scoped
  (is (= "urn:adam:user:user-1"
         (identity/user-urn "user-1")))
  (is (= "urn:adam:session:user-1:pi:session-1"
         (identity/session-urn "user-1" "session-1")))
  (is (= "urn:adam:session:user-1:claude-code:session-1"
         (identity/session-urn "user-1" "claude-code" "session-1")))
  (is (= "urn:adam:entry:user-1:pi:session-1:entry-1"
         (identity/entry-urn "user-1" "session-1" "entry-1")))
  (is (= "urn:adam:entry:user-1:claude-code:session-1:entry-1"
         (identity/entry-urn "user-1" "claude-code" "session-1" "entry-1")))
  (is (= "urn:adam:observation:user-1:pi:session-1:pi-observational-memory:memory-1"
         (identity/observation-urn
          "user-1" "pi" "session-1" "pi-observational-memory" "memory-1")))
  (is (= "urn:adam:reflection:user-1:pi:session-1:pi-observational-memory:memory-1"
         (identity/reflection-urn
          "user-1" "pi" "session-1" "pi-observational-memory" "memory-1")))
  (is (= "urn:adam:observation:user-1:claude-code:session%3A1:org.example.memory:abc123def456"
         (identity/observation-urn
          "user-1" "claude-code" "session:1" "org.example.memory" "abc123def456")))
  (is (not=
       (identity/observation-urn
        "user-1" "claude-code" "session-1" "producer-a" "abc123def456")
       (identity/observation-urn
        "user-1" "claude-code" "session-1" "producer-b" "abc123def456")))
  (let [stream-id (identity/memory-stream-urn
                   "user-1" "claude-code" "session-1" "org.example.memory")]
    (is (= "urn:adam:memory-stream:user-1:claude-code:session-1:org.example.memory"
           stream-id))
    (is (= "urn:adam:memory-record:423ed55f84938edca9ce82f080ad49465a4aea461d04545a1d8972ad8f782124"
           (identity/memory-record-urn stream-id 0)))))

(deftest git-email-normalization-preserves-display-spelling
  (is (= {:id "urn:adam:identity:git-email:09eafe4c2b195fc27f6f2dbeaf7f15aec6b293a22356951db37b56ca8c3e7c7e"
          :kind "git-email"
          :value "Linus@Example.COM"
          :normalized-value "linus@example.com"
          :display-value "Linus@Example.COM"}
         (identity/git-email-identity "  Linus@Example.COM  ")))
  (is (nil? (identity/git-email-identity "   "))))
