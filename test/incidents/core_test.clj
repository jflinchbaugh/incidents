(ns incidents.core-test
  (:require [incidents.core :refer :all]
            [incidents.db :as db]
            [incidents.report :as report]
            [clojure.test :as t]
            [tick.core :as tc]
            [chime.core :as chime]
            [clojure.java.io :as io]
            [test-with-files.tools :refer [with-tmp-dir]]))

(t/deftest test-server
  (with-tmp-dir tmp-dir
    (let [file-cfg (-> db/db-config
                       (assoc-in [:store :path] (str (io/file tmp-dir "db")))
                       (assoc-in [:store :id] (java.util.UUID/randomUUID)))
          out-dir (str (io/file tmp-dir "out"))
          schedules (atom [])
          orig-start-db! db/start-db!
          start-count (atom 0)]
      (with-redefs [db/db-config file-cfg
                    db/start-db! (fn [& args]
                                   (when (seq args)
                                     (swap! start-count inc))
                                   (apply orig-start-db! args))
                    feed-url (io/resource "incidents/feed-1.xml")
                    chime/chime-at (fn [_ f & _] (swap! schedules conj f) nil)
                    wait-forever (fn []
                                   (t/is (= 2 (count @schedules)))
                                   (t/is (nil? @db/db-conn)
                                         "db is closed when server is idle")
                                   (let [[load-fn clerk-fn] @schedules]
                                     (load-fn (tc/now))
                                     (t/is (nil? @db/db-conn)
                                           "db closed after load-fn")
                                     (t/is (= 3 (clerk-fn (tc/now))))
                                     (t/is (nil? @db/db-conn)
                                           "db closed after clerk-fn")
                                     (with-redefs [feed-url
                                                   (io/resource
                                                    "incidents/feed-2.xml")]
                                       (load-fn (tc/now))
                                       (t/is (nil? @db/db-conn)
                                             "db closed after second load-fn")
                                       (t/is (= 4 (clerk-fn (tc/now))))
                                       (t/is (nil? @db/db-conn)
                                             "db closed after 2nd clerk-fn"))))
                    report/build-clerk! (fn [_]
                                          (db/with-db [node]
                                            (count (db/get-all-facts node))))]
        ((get disconnected-actions "server") ["60" "300" out-dir])
        (t/is (= 4 @start-count) "db is opened and closed per event")))))

(t/deftest test-server-error-handler
  (with-tmp-dir tmp-dir
    (let [file-cfg (-> db/db-config
                       (assoc-in [:store :path] (str (io/file tmp-dir "db")))
                       (assoc-in [:store :id] (java.util.UUID/randomUUID)))
          out-dir (str (io/file tmp-dir "out"))
          options (atom [])]
      (with-redefs [db/db-config file-cfg
                    chime/chime-at (fn [_ _ & [opts]]
                                     (swap! options conj opts)
                                     nil)
                    wait-forever (fn [] nil)]
        ((get disconnected-actions "server") ["60" "300" out-dir])
        (t/is (= 2 (count @options)))
        (doseq [opt @options]
          (t/is (fn? (:error-handler opt)))
          (t/is (true? ((:error-handler opt)
                        (Exception. "simulated failure")))))))))
