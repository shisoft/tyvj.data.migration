(ns tyvj2joyoi.core
  (:gen-class)
  (:require [clojure.java.jdbc :as sql]
            [clojure.stacktrace]
            [less.awful.ssl :as ssl]
            [org.httpkit.client :as http]
            [cheshire.core :as json])
  (:import (java.util Date)
           (java.util Base64 UUID)
           (org.jsoup Jsoup)
           (org.jsoup.safety Whitelist)
           (org.jsoup.nodes Document$OutputSettings)
           (java.io File)))

(def configs (read-string (slurp "config.edn")))
(def base-conn-str (:base-conn-str configs))
(def joyoi-oj-db (assoc (:joyoi-oj-db configs)
                   :connection-uri (str base-conn-str "joyoi_oj")))
(def joyoi-mgmtsvc-db (assoc joyoi-oj-db
                        :connection-uri (str base-conn-str "joyoi_mgmtsvc")))
(def joyoi-blog-db (assoc joyoi-oj-db
                     :connection-uri (str base-conn-str "joyoi_blog")))
(def tyvj-db (merge joyoi-oj-db
                    (:tyvj-db configs)))
(def mgmtsvc-restore-db (:restore-db configs))
(def mgmtsvc-restore-conn (sql/get-connection mgmtsvc-restore-db))
(def joyoi-oj-conn (sql/get-connection joyoi-oj-db))
(def joyoi-mgmtsvc-conn (sql/get-connection joyoi-mgmtsvc-db))
(def tyvj-conn (doto (sql/get-connection tyvj-db) (.setAutoCommit false)))

(defn mgmt-ssl-engine []
  (let [passwd (char-array "123456")]
    (ssl/ssl-context->engine
      (ssl/ssl-p12-context
        "/home/shisoft/Documents/JoyOI/webapi-client.pfx"
        passwd
        "/home/shisoft/Documents/JoyOI/ca.cert"))))

(defn put-blob [remark s]
  (try
    (let [ssl-eng (mgmt-ssl-engine)
          s-bytes (.getBytes s)
          response @(http/request
                      {:method :put :sslengine ssl-eng
                       :url "https://mgmtsvc.1234.sh/api/v1/Blob"
                       :headers {"Content-Type" "application/json"}
                       :body (json/generate-string
                               {:Remark remark
                                :Body (.encodeToString
                                        (Base64/getEncoder) s-bytes)})
                       :timeout 600000})
          status (:status response)]
      (assert (= status 200) response)
      [(get-in (json/parse-string (:body response) true)
               [:data :id])
       (count s-bytes)])
    (catch Exception e
      (let [msg (str "Error on " remark "length" (count s) "msg:" (.getMessage e))]
        (throw (Exception. msg))))))

(defn get-blob [blob-id]
  (try
    @(http/request
       {:method :get :sslengine (mgmt-ssl-engine)
        :url (str "https://mgmtsvc.1234.sh/api/v1/Blob/" blob-id)})))

(defn gen-uuid []
  (let [temp-id (UUID/randomUUID)]
    (.toString
      (UUID. (System/currentTimeMillis)
             (.getLeastSignificantBits temp-id)))))

(defn import-tyvj-test-case [row]
  (let [{:keys [id problem_id type input output]} row]
    (println "problem:" problem_id "case id:" id "size:" (+ (count input) (count output)))
    (when (and (< type 3)
               input output)
      (let [joyoi-problem-id (str "tyvj-" problem_id)
            joyoi-type (if (= 0 type) 0 1)
            [joyoi-input
             joyoi-input-size]
            (put-blob (str joyoi-problem-id "-input") input)
            [joyoi-output
             joyoi-output-size]
            (put-blob (str joyoi-problem-id "-output") output)
            case-id (gen-uuid)]
        (try
          (sql/insert!
            joyoi-oj-db :testcases
            {:Id               case-id
             :inputblobid      joyoi-input
             :InputSizeInByte  joyoi-input-size
             :outputblobid     joyoi-output
             :OutputSizeInByte joyoi-output-size
             :problemid        joyoi-problem-id
             :Type             joyoi-type})
          (catch Exception e
            (println "Error on insertion")
            (clojure.stacktrace/print-cause-trace e)))))))

(defn migrate-test-cases []
  (let [rows (first (second (sql/query tyvj-db ["select count(*) from test_cases"]
                                       {:as-arrays? true})))
        batch-size 128
        batches (int (inc (/ rows batch-size)))
        cursor (atom 0)]
    (println "importing" rows "rows, batch size" batch-size
             "batches" batches)
    (doseq [index (range batches)]
      (println "batch" index "of" batches "from" @cursor)
      (let [testcase-stmt (sql/prepare-statement
                            tyvj-conn (str "SELECT * FROM test_cases "
                                           "WHERE id > " @cursor " "
                                           "ORDER BY id LIMIT " batch-size)
                            {:timeout 600000})
            batch-rows (sql/query tyvj-db [testcase-stmt])]
        (reset! cursor (:id (last batch-rows)))
        (println "cursor for next batch is" @cursor)
        (dorun
          (pmap import-tyvj-test-case batch-rows))))))

(defn recalculate-user-problems []
  (dorun
    (sql/query
      joyoi-oj-db
      ["SELECT Id FROM aspnetusers"]
      {:row-fn
       (fn [user]
         (let [user-id (:id user)
               judges (sql/query joyoi-oj-db
                                 ["SELECT ProblemId, Result FROM judgestatuses WHERE UserId = ? AND IsSelfTest = FALSE" user-id])
               problems (group-by :problemid judges)
               problem-passed? (for [[id results] problems]
                                 [id (some (fn [{:keys [result]}] (zero? result)) results)])
               attempted-problems (vec (keys problems))
               passed-problems (vec (map first (filter second problem-passed?)))]
           (println "Id:" user-id
                    ", attempted (" (count attempted-problems) "):"
                    attempted-problems
                    ", passed (" (count passed-problems) "):"
                    passed-problems)
           (sql/execute!
             joyoi-oj-db
             ["UPDATE aspnetusers SET PassedProblems = ?, TriedProblems = ? WHERE Id = ?"
              (json/generate-string passed-problems)
              (json/generate-string attempted-problems)
              user-id])))})))

(defn check-fix-blob-integrity []
  (let [rows (first (second (sql/query joyoi-oj-db ["select count(*) from testcases"]
                                       {:as-arrays? true})))
        batch-size 128
        batches (int (inc (/ rows batch-size)))
        cursor (atom "")]
    (println "checking" rows "rows, batch size" batch-size
             "batches" batches)
    (doseq [index (range batches)]
      (println "batch" index "of" batches "from" @cursor)
      (let [testcase-stmt (sql/prepare-statement
                            joyoi-oj-conn (str "SELECT * FROM testcases "
                                               "WHERE id > '" @cursor "' "
                                               "ORDER BY id LIMIT " batch-size)
                            {:timeout 600000})
            batch-rows (sql/query tyvj-db [testcase-stmt])
            fixed (atom #{})
            fix-problem-test-cases
            (fn [problem-id]
              (println "Fixing:" problem-id)
              (sql/execute! joyoi-oj-db ["DELETE FROM testcases WHERE ProblemId = ?" problem-id])
              (dorun
                (pmap
                  import-tyvj-test-case
                  (sql/query tyvj-db ["SELECT * FROM test_cases WHERE problem_id = ?"
                                      (read-string (.replace ^String problem-id "tyvj-" ""))]))))]
        (reset! cursor (:id (last batch-rows)))
        (println "cursor for next batch is" @cursor)
        (dorun
          (pmap
            (fn [row]
              (let [{:keys [inputblobid outputblobid problemid]} row
                    i-blob-res (get-blob inputblobid)
                    o-blob-res (get-blob outputblobid)]
                (when (and (or (= 404 (:status i-blob-res))
                               (= 404 (:status o-blob-res)))
                           (not (@fixed problemid)))
                  (println "WARNING. FAILED BLOB CHECK:" problemid)
                  (swap! fixed conj problemid)
                  (fix-problem-test-cases problemid))))
            batch-rows))))))

(defn recover-blob-integrity []
  (let [uuid-regex #"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}"
        files (.listFiles (File. "recover.blob"))
        ids-to-fix (into #{}
                         (apply concat
                                (map (fn [f]
                                       (->> (.getAbsolutePath f)
                                            (slurp)
                                            (re-seq uuid-regex))) files)))
        select-blob-by-blob-id "select * from blobs where BlobId = ?"
        query-production (sql/prepare-statement joyoi-mgmtsvc-conn select-blob-by-blob-id)
        query-restore (sql/prepare-statement mgmtsvc-restore-conn select-blob-by-blob-id)
        insert-row (fn [row] (sql/insert! joyoi-mgmtsvc-conn :blobs row))]
    (println "recovering" (count ids-to-fix) "blobs" ids-to-fix)
    (doseq [blob-id ids-to-fix]
      (println "checking" blob-id)
      (when (empty? (sql/query joyoi-mgmtsvc-db [query-production blob-id]))
        (println "found missing" blob-id)
        (let [rows (sql/query mgmtsvc-restore-db [query-restore blob-id])]
          (println "found" (count rows) "rows to recover")
          (doseq [{:keys [id] :as row} rows]
            (try
              (println "inserting row:" row)
              (insert-row row)
              (catch Exception e
                (println "retrying inserting row:" row)
                (insert-row (assoc row
                              :id (gen-uuid)))))))))))


(defn migrate-problems []
  (let [problem-stmt (sql/prepare-statement
                       tyvj-conn "select * from problems"
                       {:fetch-size 10})
        max-memory (* 1204 1024 512)                    ; 512MB
        illegal-problems (atom [])]
    ; Problem migration
    (dorun
        (sql/query
          tyvj-db [problem-stmt]
          {:row-fn
           (fn [row]
             (let [{:keys [id title background description input output hint
                           memory_limit time_limit
                           submit_count accepted_count hide]} row
                   title (apply str (take 128 (seq title)))
                   memory_limit (* memory_limit 1024)
                   region (fn [title content]
                            (when (seq? (seq content))
                              (str "\n \n # " title " \n"
                                   (.trim content) " \n")))
                   joyoi-id (str "tyvj-" id)
                   joyoi-body (str (region "题目背景" background)
                                   (region "题目描述" description)
                                   (region "输入格式" input)
                                   (region "输出格式" output)
                                   (region "提示" hint))
                   mgmt-blob (str "https://mgmtsvc.1234.sh/api/v1/Blob")
                   memory_limit (if (> memory_limit max-memory)
                                  (do (swap! illegal-problems conj id)
                                      max-memory)
                                  memory_limit)]
               (println title " - " hide)
               (try
                 (sql/insert! joyoi-oj-db :problems
                              {:Id                            joyoi-id
                               :Body                          joyoi-body
                               :CachedAcceptedCount           accepted_count
                               :CachedSubmitCount             submit_count
                               :IsVisiable                    (not hide)
                               :MemoryLimitationPerCaseInByte memory_limit
                               :Source                        0
                               :TimeLimitationPerCaseInMs     time_limit
                               :Title                         title
                               :Tags                          "按题库:本地"
                               :Difficulty                    0
                               :CreatedTime                   (Date.)})
                 (catch Exception e
                   (println "Error on insertion. id:" id "title:" title
                            "mem" memory_limit "hide:" hide)
                   (clojure.stacktrace/print-cause-trace e)))))})
        (println "Illegal problems:" @illegal-problems))))


(defn strip-html [html]
  (let [frag (Jsoup/clean html ""
                          (-> (Whitelist/none)
                              (.addTags  (into-array ["p" "br"])))
                          (-> (Document$OutputSettings.)
                              (.prettyPrint true)))]
    (Jsoup/clean frag "", (Whitelist/none)
                 (-> (Document$OutputSettings.)
                     (.prettyPrint false)))))

(def solution-user-id "08d53895-e8c0-4f07-6579-a8af8ec273af")
(defn migrate-solutions []
  (let [get-user-name
        (fn [user-id]
          (first (second (sql/query tyvj-db ["SELECT username FROM users WHERE id = ?" user-id]
                                    {:as-arrays? true}))))
        get-problem-title
        (fn [problem-id]
          (first (second (sql/query tyvj-db ["SELECT title FROM problems WHERE id = ?" problem-id]
                                    {:as-arrays? true}))))
        rand-chars (concat (range 10)
                           (map #(char (+ 65 %)) (range 26)))
        rand-str
        (fn [len]
          (apply str (take len (repeatedly #(rand-nth rand-chars)))))
        get-summary
        (fn [content code user-name]
          (let [summary-lines 5
                stripped-html (strip-html content)
                content-lines (.split stripped-html "\n")
                top-content-lines (take summary-lines content-lines)
                top-lines-content (clojure.string/join "\n\n" top-content-lines)
                code-lines (.split code "\n")]
            (if (= 5 (count content-lines))
              top-lines-content
              (str top-lines-content
                   "```"
                   (take (- 5 (content-lines)) code-lines)
                   "```"
                   (when (< (count code-lines)
                            (- 5 (content-lines)))
                     (str "作者：" user-name))))))]

    (doseq [{:keys [id problem_id user_id title content code language]
             :as solution} (sql/query tyvj-db ["SELECT * FROM solutions"])]
      (when (and code (not= (clojure.string/trim code) ""))
        (let [user-name (get-user-name user_id)
              problem-title (get-problem-title problem_id)
              joyoi-content (clojure.string/trim
                              (str (clojure.string/trim content) "\n\n"
                                   (str "```"  "\n"
                                        code  "\n"
                                        "```"  "\n")
                                   "作者：" user-name))
              joyoi-post {:Id (gen-uuid)
                          :Content joyoi-content
                          :Summary (get-summary content code user-name)
                          :IsPage false
                          :ProblemId (str "tyvj-" problem_id)
                          :ProblemTitle problem-title
                          :Time (Date.)
                          :Title (if (or (nil? title)
                                         (= "" (clojure.string/trim title)))
                                   (str problem-title " 题解")
                                   title)
                          :Url (clojure.string/lower-case (rand-str 8))
                          :UserId solution-user-id}]
          (println "Insert:" (:Title joyoi-post)
                   "for:" problem_id)
          (sql/insert! joyoi-blog-db :posts joyoi-post))))))

(defn -main
  [& args]
  )
