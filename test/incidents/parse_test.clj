(ns incidents.parse-test
  (:require [incidents.parse :refer :all]
            [clojure.test :as t]
            [feedparser-clj.core :as feed]
            [tick.core :as tc]
            [clojure.java.io :as io]
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
          :uri "021cb6cb-b2bc-405a-86d9-73376696bc14"}))))
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
          :uri "021cb6cb-b2bc-405a-86d9-73376696bc14"}))))
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
          :uri "021cb6cb-b2bc-405a-86d9-73376696bc14"}))))
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
          :uri "021cb6cb-b2bc-405a-86d9-73376696bc14"})))))

(t/deftest test-tag
  (t/is (= {:thing :value :type :fact} (tag :fact {:thing :value}))))

(t/deftest test-prune
  (t/is (=
         {:thing :value
          :uri "uri"
          :nested {:type :fact :uri "uri" :empty ""}}
         (prune
          {:thing :value
           :uri "uri"
           :nested {:type :fact :uri "uri" :empty ""}
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
