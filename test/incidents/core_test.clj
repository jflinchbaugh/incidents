(ns incidents.core-test
  (:require [incidents.core :refer :all]
            [clojure.test :as t]
            [feedparser-clj.core :as feed]
            [tick.core :as tc]
            [datahike.api :as d]
            [chime.core :as chime]
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

(defn mem-db-config []
  (assoc db-config
         :store {:backend :memory
                 :id (java.util.UUID/randomUUID)}))

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

(t/deftest test-everything
  (with-db [node (mem-db-config)]
    (t/is node "the node is open")

    (load-feed! node (io/resource "incidents/feed-1.xml"))
    (t/is
      (= 1 (count (get-feeds-since node (get-last-feed-time node))))
      "feeds has a doc")

    (t/is (empty? (get-all-facts node)) "facts start empty")

    (t/is (= 1 (count (transform-facts! node))) "fact transactions")
    (t/is (= 3 (count (get-all-facts node))) "facts are loaded from staging")

    (load-feed! node (io/resource "incidents/feed-1.xml"))
    (t/is (empty? (get-feeds-since node (get-last-feed-time node)))
      "duplicate feed is not stored")

    (load-feed! node (str (io/resource "incidents/feed-2.xml")))
    (t/is (= 1 (count (get-feeds-since node (get-last-feed-time node))))
      "feeds has 1 new doc")

    (t/is (= 1 (count (transform-facts! node))) "fact transactions")
    (t/is (= 4 (count (get-all-facts node))) "facts are loaded from staging")
    (t/is (= 3 (count (get-all-active-facts node))) "active facts updated")

    (let [fact-uri (:uri (first (get-all-facts node)))]
      (with-redefs [feed-batch-size 2]
        (t/is (= 2 (count (delete-all-facts! node)))
          "evicts facts in batches"))
      ((get connected-actions "reset") node nil)
      (t/is (empty? (get-all-facts node)) "reset clears facts")
      (t/is (nil? (d/entity (d/db node) [:incidents.fact/uri fact-uri]))
        "reset retracts fact entity")
      (t/is (= 0 (get-last-feed-time node)) "reset clears last-feed-time"))

    (t/is (= 2 (count (transform-facts! node))) "reprocess fact transactions")
    (t/is (= 4 (count (get-all-facts node))) "facts are reprocessed")))

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
         (keys->db "incidents.feed")
         (keys->mem))))))

(t/deftest test-xtdb
  (with-db [node (mem-db-config)]
    (t/is node "node is open")
    (t/is (put-feed! node {:doc ""}))
    (t/is (nil? (put-feed! node {:doc ""})))
    (t/is (= 1 (count (get-feeds-since node 0)))))
  (with-tmp-dir tmp-dir
    (let [file-cfg (-> db-config
                       (assoc-in [:store :path] (str (io/file tmp-dir "db")))
                       (assoc-in [:store :id] (java.util.UUID/randomUUID)))]
      (with-db [node file-cfg]
        (t/is (put-feed! node {:doc "persisted"})))
      (with-db [node file-cfg]
        (t/is (= 1 (count (get-feeds-since node 0))))))))

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

(t/deftest test-last-feed-time
  (with-db [node (mem-db-config)]
    (let [now (unix-time (tc/now))
          earlier (- now 1)
          _ (put-last-feed-time! node earlier)
          _ (put-last-feed-time! node now)]
      (t/is (= now (get-last-feed-time node))))))

(t/deftest test-export-import
  (with-tmp-dir tmp-dir
    (with-db [node (mem-db-config)]
      (let [t1 (tc/instant "2022-08-11T03:40:00Z")
            t2 (tc/instant "2022-08-11T03:45:00Z")
            t3 (tc/instant "2022-08-11T03:50:00Z")
            doc-1 (slurp (io/resource "incidents/feed-1.xml"))
            doc-2 (slurp (io/resource "incidents/feed-2.xml"))
            file (str (io/file tmp-dir "feeds.edn.gz"))]
        (put-feed! node t1 doc-1)
        (put-feed! node t3 doc-2)
        (let [orig-feeds (get-feeds-since node 0)]
          (t/is (= [(str t1) (str t3)] (mapv :date orig-feeds)))
          ((get connected-actions "export") node [file])
          (with-redefs [feed-batch-size 1]
            (t/is (= 2 (count ((get connected-actions "purge-feeds")
                               node
                               nil)))))
          (t/is (empty? (get-feeds-since node 0)))
          (t/is (nil? (d/entity
                       (d/db node)
                       [:incidents.feed/date (:date (first orig-feeds))])))
          (t/is (str/includes?
                 (with-out-str
                   ((get connected-actions "import") node [file]))
                 (str "Importing 2 for " t1)))
          (t/is (= orig-feeds (get-feeds-since node 0)))
          ((get connected-actions "purge-feeds") node nil)
          (with-open [out (-> file
                              io/output-stream
                              java.util.zip.GZIPOutputStream.
                              io/writer)]
            (binding [*out* out]
              (prn {:date (str t1) :doc doc-1})
              (prn {:date (str t2) :doc doc-1})
              (prn {:date (str t3) :doc doc-2})))
          ((get connected-actions "import") node [file])
          (t/is (= orig-feeds (get-feeds-since node 0))))))))

(t/deftest test-server
  (with-tmp-dir tmp-dir
    (let [file-cfg (-> db-config
                       (assoc-in [:store :path] (str (io/file tmp-dir "db")))
                       (assoc-in [:store :id] (java.util.UUID/randomUUID)))
          out-dir (str (io/file tmp-dir "out"))
          schedules (atom [])]
      (with-redefs [db-config file-cfg
                    feed-url (io/resource "incidents/feed-1.xml")
                    chime/chime-at (fn [_ f] (swap! schedules conj f) nil)
                    wait-forever (fn [] nil)
                    build-clerk! (fn [_]
                                   (with-db [node]
                                     (count (get-all-facts node))))]
        ((get disconnected-actions "server") ["60" "300" out-dir])
        (t/is (= 2 (count @schedules)))
        (let [[load-fn clerk-fn] @schedules]
          (load-fn (tc/now))
          (t/is (= 3 (clerk-fn (tc/now))))
          (with-redefs [feed-url (io/resource "incidents/feed-2.xml")]
            (load-fn (tc/now))
            (t/is (= 4 (clerk-fn (tc/now))))))))))

(t/deftest test-gc
  (with-tmp-dir tmp-dir
    (let [db-dir (io/file tmp-dir "db")
          file-cfg (-> db-config
                       (assoc-in [:store :path] (str db-dir))
                       (assoc-in [:store :id] (java.util.UUID/randomUUID)))
          ksv-count (fn []
                      (count
                       (filter
                        (fn [f] (str/ends-with? (.getName f) ".ksv"))
                        (.listFiles db-dir))))]
      (with-db [node file-cfg]
        (let [base-files (ksv-count)]
          (doseq [i (range 10)]
            (put-feed!
             node
             (tc/>> (tc/epoch) (tc/new-duration i :minutes))
             (str "feed-" i)))
          ((get connected-actions "purge-feeds") node nil)
          ((get connected-actions "gc") node nil)
          (t/is (<= (ksv-count) (+ base-files 2))))))))
