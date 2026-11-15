(ns incidents.report-test
  (:require [incidents.report :refer :all]
            [clojure.test :as t]
            [tick.core :as tc]
            [clojure.java.io :as io]
            [test-with-files.tools :refer [with-tmp-dir]]
            [clojure.string :as str]))

(t/deftest test-report
  (let [dt (tc/date-time "2022-08-11T03:42:39")
        fact {:incident-type :medical
              :municipality "Salisbury Township"
              :streets ["Chestnut St" "McBridge St"]
              :start-date dt
              :title "Medical Emergency"
              :units ["Med 293 Chester" "Amb 49-2"]}]
    (t/is (= "2022-08-11" (format-date dt)))
    (t/is (= "2022-08-11 03:42:39" (format-date-time dt)))
    (t/is (= "Chestnut St & McBridge St"
             (format-streets (:streets fact))))
    (t/is (= "" (format-streets nil)))
    (t/is (= "Chestnut St Salisbury Township PA"
             (format-map-location "Salisbury Township" ["Chestnut St"])))
    (t/is (= (str "https://www.google.com/maps/search/"
                  "?api=1&query=Chestnut+St+Salisbury+Township+PA")
             (str (map-link "Salisbury Township" ["Chestnut St"]))))
    (t/is (= "Medical" (format-incident-type :medical)))
    (with-tmp-dir tmp-dir
      (report-active [fact] tmp-dir)
      (let [html (slurp (io/file tmp-dir "index.html"))]
        (t/is (str/includes? html "Active Incidents"))
        (t/is (str/includes? html "Medical (1)"))
        (t/is (str/includes? html "Traffic (0)"))
        (t/is (str/includes? html "Fire (0)"))
        (t/is (str/includes? html "Salisbury Township"))
        (t/is (str/includes? html "Chestnut St & McBridge St"))
        (t/is (str/includes? html "Med 293 Chester, Amb 49-2"))))))
