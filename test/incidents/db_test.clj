(ns incidents.db-test
  (:require [incidents.db :refer :all]
            [clojure.test :as t]
            [tick.core :as tc]
            [datahike.api :as d]
            [clojure.java.io :as io]
            [test-with-files.tools :refer [with-tmp-dir]]
            [clojure.string :as str]
            [taoensso.telemere :as log]))

(defn mem-db-config []
  (assoc db-config
         :store {:backend :memory
                 :id (java.util.UUID/randomUUID)}))

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
      (reset-facts! node)
      (t/is (empty? (get-all-facts node)) "reset clears facts")
      (t/is (nil? (d/entity (d/db node) [:incidents.fact/uri fact-uri]))
        "reset retracts fact entity")
      (t/is (= 0 (get-last-feed-time node)) "reset clears last-feed-time"))

    (t/is (= 1 (count (transform-facts! node)))
      "reprocess fact transactions in one batch")
    (t/is (= 4 (count (get-all-facts node))) "facts are reprocessed")
    (t/is (= 3 (count (get-all-active-facts node)))
      "active facts updated after batch reprocess")
    (with-redefs [feed-batch-size 1]
      (reset-facts! node)
      (t/is (= 2 (count (transform-facts! node)))
        "reprocess fact transactions across batches"))))

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
    (t/is (:fuse-index-roots? (:config (d/db node))))
    (t/is (get-in (d/db node) [:schema :incidents.fact/active? :db/index]))
    (t/is (number? (get-in (d/db node)
                           [:ident-ref-map :incidents.feed/doc])))
    (t/is (thrown? Exception
                   (d/transact node [{:incidents.feed/unknown "x"}])))
    (t/is (put-feed! node ""))
    (t/is (nil? (put-feed! node "")))
    (t/is (= 1 (count (get-feeds-since node 0)))))
  (with-tmp-dir tmp-dir
    (let [file-cfg (-> db-config
                       (assoc-in [:store :path] (str (io/file tmp-dir "db")))
                       (assoc-in [:store :id] (java.util.UUID/randomUUID)))]
      (with-db [node file-cfg]
        (t/is (put-feed! node "persisted")))
      (with-db [node file-cfg]
        (t/is (= 1 (count (get-feeds-since node 0))))))))

(t/deftest test-lmdb-storage
  (with-tmp-dir tmp-dir
    (let [lmdb-dir (io/file tmp-dir "lmdb")
          lmdb-cfg (-> db-config
                       (assoc :store {:backend :lmdb
                                      :path (str lmdb-dir)
                                      :id (java.util.UUID/randomUUID)}))]
      (with-db [node lmdb-cfg]
        (t/is (put-feed! node "lmdb-persisted")))
      (t/is (.exists (io/file lmdb-dir "data.mdb"))
            "LMDB data file was created")
      (with-db [node lmdb-cfg]
        (t/is (= 1 (count (get-feeds-since node 0))))))))

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
          (export-feeds! node file)
          (with-redefs [feed-batch-size 1]
            (t/is (= 2 (count (delete-all-feeds! node)))))
          (t/is (empty? (get-feeds-since node 0)))
          (t/is (nil? (d/entity
                       (d/db node)
                       [:incidents.feed/date (:date (first orig-feeds))])))
          (t/is (str/includes?
                 (with-out-str
                   (import-feeds! node file))
                 (pr-str {:importing 2 :date (str t1)})))
          (t/is (= orig-feeds (get-feeds-since node 0)))
          (delete-all-feeds! node)
          (with-open [out (-> file
                              io/output-stream
                              java.util.zip.GZIPOutputStream.
                              io/writer)]
            (binding [*out* out]
              (prn {:date (str t1) :doc doc-1})
              (prn {:date (str t2) :doc doc-1})
              (prn {:date (str t3) :doc doc-2})))
          (import-feeds! node file)
          (t/is (= orig-feeds (get-feeds-since node 0))))))))

(t/deftest test-gc
  (with-tmp-dir tmp-dir
    (let [db-dir (io/file tmp-dir "db")
          file-cfg (-> db-config
                       (assoc :store {:backend :file
                                      :path (str db-dir)
                                      :id (java.util.UUID/randomUUID)})
                       (assoc-in [:index-config :branching-factor] 8))
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
          (delete-all-feeds! node)
          (with-redefs [db-config (assoc-in db-config
                                            [:online-gc :grace-period-ms]
                                            0)]
            (gc! node))
          (t/is (<= (ksv-count) (+ base-files 2))))))))

(t/deftest test-datahike-logging
  (with-db [node (mem-db-config)]
    (t/is
     (= ""
        (with-out-str
          (d/q
           '[:find ?e :where [?e :incidents.feed/type :feed]]
           (d/db node)
           :ignored)))))
  (t/is
   (str/includes?
    (log/with-ns-filter
      #{"datahike.*"}
      (log/with-min-level
        :debug
        (with-out-str
          (with-db [node (mem-db-config)]))))
    ":datahike/connect")))

(t/deftest test-load-feed-resilience
  (with-db [node (mem-db-config)]
    (t/testing "invalid or unreachable source returns nil and does not throw"
      (t/is (nil? (load-feed!
                   node
                   "http://invalid-host-does-not-exist.example/feed")))
      (t/is (nil? (load-feed! node "non-existent-file.xml"))))
    (with-tmp-dir tmp-dir
      (t/testing "empty feed file returns nil and does not transact"
        (let [empty-file (io/file tmp-dir "empty.xml")]
          (spit empty-file "   \n  ")
          (t/is (nil? (load-feed! node empty-file)))
          (t/is (empty? (get-feeds-since node 0))
                "no empty feed was transacted"))))))
