(ns incidents.core-test
  (:require [incidents.core :refer :all]
            [clojure.test :as t]))

(t/deftest test-parse
  (t/testing "parse null values"
    (t/is
      (= {:uri "021cb6cb-b2bc-405a-86d9-73376696bc14"
          :start-date #inst "2022-08-11T03:42:39.000-00:00"
          :title "Medical Emergency"
          :municipality "Salisbury Township"
          :streets []
          :units []
          }
        (parse
          {:description
           {:type "text/html"
            :value
            "SALISBURY TOWNSHIP"}
           :link "http://www.lcwc911.us/lcwc/lcwc/publiccad.asp",
           :published-date #inst "2022-08-11T03:42:39.000-00:00"
           :title "MEDICAL EMERGENCY"
           :uri "021cb6cb-b2bc-405a-86d9-73376696bc14"
           :type :stage
           }))))
  (t/testing "parse all the values"
    (t/is
      (= {:uri "021cb6cb-b2bc-405a-86d9-73376696bc14"
          :start-date #inst "2022-08-11T03:42:39.000-00:00"
          :title "Medical Emergency"
          :municipality "Salisbury Township"
          :streets ["Chestnut St" "Bridge St" "Another Rd"]
          :units ["Med 293 Chester" "Amb 49-2"]
          }
        (parse
          {:description
           {:type "text/html"
            :value
            "SALISBURY TOWNSHIP;  CHESTNUT ST & BRIDGE ST / ANOTHER RD; MED 293 CHESTER<br> AMB 49-2; "}
           :link "http://www.lcwc911.us/lcwc/lcwc/publiccad.asp",
           :published-date #inst "2022-08-11T03:42:39.000-00:00"
           :title "MEDICAL EMERGENCY"
           :uri "021cb6cb-b2bc-405a-86d9-73376696bc14"
           :type :stage
           })))))

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
