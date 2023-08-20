(ns incidents.core-test
  (:require [incidents.core :refer :all]
            [clojure.test :as t]
            [feedparser-clj.core :as feed]
            [tick.core :as tc]
            [xtdb.api :as xt]
            [clojure.java.io :as io]
            [test-with-files.tools :refer [with-tmp-dir]]
            [clojure.string :as str]))

(t/deftest test-parse
  (t/testing "parse null values"
    (t/is
     (= {:uri "021cb6cb-b2bc-405a-86d9-73376696bc14"
         :start-date #inst "2022-08-11T03:42:39.000-00:00"
         :active? true
         :title "Medical Emergency"
         :incident-type :medical
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
         :active? true
         :title "Medical Emergency"
         :incident-type :medical
         :municipality "Salisbury Township"
         :streets ["Chestnut St" "McBridge St" "Anomcer Rd"]
         :units ["Med 293 Chester" "Amb 49-2"]}
        (parse
         {:description
          {:type "text/html"
           :value
           "SALISBURY TOWNSHIP;  CHESTNUT ST & MCBRIDGE ST / ANOMCER RD; MED 293 CHESTER<br> AMB 49-2; "}
          :link "http://www.lcwc911.us/lcwc/lcwc/publiccad.asp",
          :published-date #inst "2022-08-11T03:42:39.000-00:00"
          :title "MEDICAL EMERGENCY"
          :uri "021cb6cb-b2bc-405a-86d9-73376696bc14"
          :type :stage}))))
  (t/testing "parse all the values and trim"
    (t/is
     (= {:uri "021cb6cb-b2bc-405a-86d9-73376696bc14"
         :start-date #inst "2022-08-11T03:42:39.000-00:00"
         :active? true
         :title "Medical Emergency-Whatever"
         :incident-type :medical
         :municipality "Salisbury Township"
         :streets ["Chestnut St" "McBridge St" "Anomcer Rd"]
         :units ["Med 293 Chester" "Amb 49-2"]}
        (parse
         {:description
          {:type "text/html"
           :value
           " SALISBURY TOWNSHIP ;  CHESTNUT ST & MCBRIDGE ST / ANOMCER RD ;  MED 293 CHESTER <br> AMB 49-2 ; "}
          :link "http://www.lcwc911.us/lcwc/lcwc/publiccad.asp",
          :published-date #inst "2022-08-11T03:42:39.000-00:00"
          :title " MEDICAL EMERGENCY-WHATEVER "
          :uri "021cb6cb-b2bc-405a-86d9-73376696bc14"
          :type :stage}))))
  (t/testing "parse county lacks streets"
    (t/is
     (= {:uri "021cb6cb-b2bc-405a-86d9-73376696bc14"
         :start-date #inst "2022-08-11T03:42:39.000-00:00"
         :active? true
         :title "Medical Emergency"
         :incident-type :medical
         :municipality "Berks County"
         :streets []
         :units ["Med 293 Chester" "Amb 49-2"]}
        (parse
         {:description
          {:type "text/html"
           :value
           "BERKS COUNTY; MED 293 CHESTER<br> AMB 49-2; "}
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

(t/deftest test-add-feed-id
  (t/is (=
          {:thing :value :uri "uri" :date "time" :xt/id {:type :feed :date "time"}}
          (add-feed-id {:thing :value :uri "uri" :date "time"}))))

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
  (let [now (tc/inst)
        start-time (tc/<< now (tc/new-duration 10 :minutes))]
    (t/is (=
           {:key :val
            :start-date start-time
            :end-date now
            :active? false
            :duration-minutes 10}
           (end now {:key :val :start-date start-time})))))

#_(t/deftest test-start-xtdb!
    (with-tmp-dir tempdir
      (with-open [node (start-xtdb! tempdir)]
        (t/is node))))

(t/deftest test-everything
  (with-open [node (xt/start-node {})]
    (t/is node "the node is open")
    (t/is (empty? (get-all-stage node)) "staging starts empty")

    (load-feed! node (str (io/resource "incidents/feed-1.xml")))
    (t/is (= 1 (count (get-feed-since node 0))) "feeds has a doc")

    (load-stage! node (str (io/resource "incidents/feed-1.xml")))
    (t/is (= 3 (count (get-all-stage node))) "staging has data")

    (t/is (empty? (get-all-facts node)) "facts start empty")

    (transform-facts! node)
    (t/is (= 3 (count (get-all-facts node))) "facts are loaded from staging")

    (load-feed! node (str (io/resource "incidents/feed-2.xml")))
    (t/is (= 2 (count (get-feed-since node 0))) "feeds has 2 docs")

    (load-stage! node (str (io/resource "incidents/feed-2.xml")))
    (t/is (= 3 (count (get-all-stage node))) "staging has data")

    (transform-facts! node)
    (t/is (= 4 (count (get-all-facts node))) "facts are loaded from staging")

    (t/is (clear-all-stage! node) "evict all of staging")
    (t/is (empty? (get-all-stage node)) "staging is again empty")

    (t/is (= 4 (count (get-all-facts node))) "facts are still there")))

(t/deftest test-keys-db-mem
  (t/testing "round trip values through keys transformation"
    (t/is
     (= {:uri "021cb6cb-b2bc-405a-86d9-73376696bc14"
         :start-date #inst "2022-08-11T03:42:39.000-00:00"
         :title "Medical Emergency-Whatever"
         :incident-type :medical
         :municipality "Salisbury Township"
         :streets ["Chestnut St" "McBridge St" "Anomcer Rd"]
         :units ["Med 293 Chester" "Amb 49-2"]}
        (->>
         {:uri "021cb6cb-b2bc-405a-86d9-73376696bc14"
          :start-date #inst "2022-08-11T03:42:39.000-00:00"
          :title "Medical Emergency-Whatever"
          :incident-type :medical
          :municipality "Salisbury Township"
          :streets ["Chestnut St" "McBridge St" "Anomcer Rd"]
          :units ["Med 293 Chester" "Amb 49-2"]}
         (keys->db "incidents.stage")
         (keys->mem))))))

(t/deftest test-xtdb
  (with-open [node (xt/start-node {})]
    (t/is node "node is open")
    (t/is (put-stage! node (tag :stage (add-stage-id {:uri 1 :thing :other}))))
    (t/is (get-all-stage node))))

(t/deftest test-parse-feed
  (let [source (->>
                "incidents/feed-1.xml"
                (io/resource)
                str)
        content (->> source feed/parse-feed)]
    (t/is (str/includes? source "file:/"))
    (t/is (str/includes? source "feed-1.xml"))
    (t/is (= 3 (count (:entries content))))))

(t/deftest test-incident-type
  (t/is (nil? (incident-type nil)))
  (t/is (nil? (incident-type {})))
  (t/are [type title] (= type (incident-type {:title title}))
    :medical "Medical Emergency"
    :medical "Something EMS"
    :medical "Emergency Transfer"
    :medical "Routine Transfer"
    :traffic "Traffic Accident"
    :traffic "Vehicle Fire"
    :fire "Gas Leak"
    :fire "Standby-Transfer"))
