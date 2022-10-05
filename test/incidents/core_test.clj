(ns incidents.core-test
  (:require [incidents.core :refer :all]
            [clojure.test :as t]
            [java-time :as jt]
            [xtdb.api :as xt]
            [clojure.java.io :as io]
            [test-with-files.tools :refer [with-tmp-dir]]))

(t/deftest test-parse
  (t/testing "parse null values"
    (t/is
     (= {:uri "021cb6cb-b2bc-405a-86d9-73376696bc14"
         :start-date #inst "2022-08-11T03:42:39.000-00:00"
         :title "Medical Emergency"
         :municipality "Salisbury Township"
         :streets []
         :units []}
        (parse
         {:description
          {:type "text/html"
           :value
           "SALISBURY TOWNSHIP"}
          :link "http://www.lcwc911.us/lcwc/lcwc/publiccad.asp",
          :published-date #inst "2022-08-11T03:42:39.000-00:00"
          :title "MEDICAL EMERGENCY"
          :uri "021cb6cb-b2bc-405a-86d9-73376696bc14"
          :type :stage}))))
  (t/testing "parse all the values"
    (t/is
     (= {:uri "021cb6cb-b2bc-405a-86d9-73376696bc14"
         :start-date #inst "2022-08-11T03:42:39.000-00:00"
         :title "Medical Emergency"
         :municipality "Salisbury Township"
         :streets ["Chestnut St" "Bridge St" "Another Rd"]
         :units ["Med 293 Chester" "Amb 49-2"]}
        (parse
         {:description
          {:type "text/html"
           :value
           "SALISBURY TOWNSHIP;  CHESTNUT ST & BRIDGE ST / ANOTHER RD; MED 293 CHESTER<br> AMB 49-2; "}
          :link "http://www.lcwc911.us/lcwc/lcwc/publiccad.asp",
          :published-date #inst "2022-08-11T03:42:39.000-00:00"
          :title "MEDICAL EMERGENCY"
          :uri "021cb6cb-b2bc-405a-86d9-73376696bc14"
          :type :stage})))))

(t/deftest test-tag
  (t/is (= {:thing :value :type :stage} (tag :stage {:thing :value}))))

(t/deftest test-add-stage-id
  (t/is (=
         {:thing :value :uri "uri" :xt/id {:type :stage :uri "uri"}}
         (add-stage-id {:thing :value :uri "uri"}))))

(t/deftest test-add-fact-id
  (t/is (=
         {:thing :value :uri "uri" :xt/id {:type :fact :uri "uri"}}
         (add-fact-id {:thing :value :uri "uri"}))))

(t/deftest test-prune
  (t/is (=
         {:thing :value
          :uri "uri"
          :xt/id {:type :fact :uri "uri" :empty ""}}
         (prune
          {:thing :value
           :uri "uri"
           :xt/id {:type :fact :uri "uri" :empty ""}
           :empty-str ""
           :empty-list []
           :empty-map {}
           :nil-field nil}))))

(t/deftest test-end
  (let [now (jt/local-date-time)
        start-time (jt/minus now (jt/minutes 10))]
    (t/is (=
           {:key :val
            :start-date start-time
            :end-date now
            :duration-minutes 10}
           (end now {:key :val :start-date start-time})))))

(t/deftest test-start-xtdb!
  (with-tmp-dir tempdir
    (with-open [node (start-xtdb! tempdir)]
      (t/is node))))

(t/deftest test-everything
  (with-open [node (xt/start-node {})]
    (t/is node "the node is open")
    (t/is (empty? (get-all-stage node)) "staging starts empty")

    (load-stage! node (str (io/resource "incidents/feed-1.xml")))
    (t/is (= 3 (count (get-all-stage node))) "staging has data")

    (t/is (empty? (get-all-facts node)) "facts start empty")

    (transform-facts! node)
    (t/is (= 3 (count (get-all-facts node))) "facts are loaded from staging")

    (load-stage! node (str (io/resource "incidents/feed-2.xml")))
    (t/is (= 3 (count (get-all-stage node))) "staging has data")

    (transform-facts! node)
    (t/is (= 4 (count (get-all-facts node))) "facts are loaded from staging")

    (t/is (clear-all-stage! node) "evict all of staging")
    (t/is (empty? (get-all-stage node)) "staging is again empty")

    (t/is (= 4 (count (get-all-facts node))) "facts are still there")
    ))

(comment

  (def test-node (xt/start-node {}))

  (xt/attribute-stats test-node)

  (.close test-node)

  .)
