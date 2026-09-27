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
  (is (= "urn:adam:observation:user-1:pi:session-1:memory-1"
         (identity/observation-urn "user-1" "pi" "session-1" "memory-1")))
  (is (= "urn:adam:reflection:user-1:pi:session-1:memory-1"
         (identity/reflection-urn "user-1" "pi" "session-1" "memory-1"))))

(deftest git-email-normalization-preserves-display-spelling
  (is (= {:id "urn:adam:identity:git-email:09eafe4c2b195fc27f6f2dbeaf7f15aec6b293a22356951db37b56ca8c3e7c7e"
          :kind "git-email"
          :value "Linus@Example.COM"
          :normalized-value "linus@example.com"
          :display-value "Linus@Example.COM"}
         (identity/git-email-identity "  Linus@Example.COM  ")))
  (is (nil? (identity/git-email-identity "   "))))
