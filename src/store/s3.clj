(ns 
#^{:doc 
"
a lib for interacting with s3.
"}
store.s3
  (:use store.core)
  (:use clj-serializer.core)
  (:import (clj_serializer Serializer)
           (java.io DataOutputStream ByteArrayOutputStream ObjectOutputStream
                    DataInputStream ByteArrayInputStream ObjectInputStream))
  (:require [clojure.contrib.duck-streams :as ds])
  (:import java.io.File)
  (:import org.jets3t.service.S3Service)
  (:import org.jets3t.service.impl.rest.httpclient.RestS3Service)
  (:import org.jets3t.service.model.S3Object)
  (:import org.jets3t.service.security.AWSCredentials)
  (:import org.jets3t.service.utils.ServiceUtils))

(defn s3-connection 
  ([{access-key :key secret-key :secretkey}] 
     (s3-connection access-key secret-key))
  ([k sk] (RestS3Service. (AWSCredentials. k sk))))

(defn buckets [s3] (.listAllBuckets s3))

(defn objects
  ([s3 bucket-name] 
     (.listObjects s3 (.getBucket s3 bucket-name)))
  ([s3 bucket-root rest] 
     (.listObjects s3 (.getBucket s3 bucket-root) rest nil)))

(defn folder? [o]
  (or (> (.indexOf o "$folder$") 0)
      (> (.indexOf o "logs") 0)))

(defn without-folders [c]
  (filter #(not (folder? (.getKey %))) c))

(defn create-bucket [s3 bucket-name]
  (.createBucket s3 bucket-name))

(defn delete-bucket [s3 bucket-name]
  (.deleteBucket s3 bucket-name))

(defn delete-object [s3 bucket-name key]
  (.deleteObject s3 bucket-name key))

(defn put-file
  ([s3 bucket-name file]
     (let [bucket (.getBucket s3 bucket-name)
	   s3-object (S3Object. bucket file)]
       (.putObject s3 bucket s3-object)))
  ([connection bucket-name key file]
     (let [bucket (.getBucket connection bucket-name)
	   s3-object (S3Object. bucket file)]
       (.setKey s3-object key)
       (.putObject connection bucket s3-object))))

(defn put-str
 [s3 bucket-name key data]
  (let [bucket (.getBucket s3 bucket-name)
        s3-object (S3Object. bucket key data)]
    (.putObject s3 bucket s3-object)))

(defn obj-to-str [obj]
  (ServiceUtils/readInputStreamToString (.getDataInputStream obj) "UTF-8"))

(defn get-str [s3 bucket-name key]
  (let [bucket (.getBucket s3 bucket-name)
        obj (.getObject s3 bucket key)]
    (obj-to-str obj)))

(defn put-clj [s3 bucket-name key clj]
  (let [bucket (.getBucket s3 bucket-name)  
        s3-object (S3Object. bucket key)
	_ (.setDataInputStream s3-object (ByteArrayInputStream. (serialize clj)))]
    (.putObject s3 bucket s3-object)))

(defn s3-deserialize
  ([is eof-val]
     (let [dis (DataInputStream. is)]
       (try 
         (Serializer/deserialize dis eof-val)
         (finally 
          (.close dis)))))
  ([obj]
     (s3-deserialize (-> obj
                         .getDataInputStream
                         .getWrappedInputStream)
                     (Object.))))

(defn get-clj [s3 bucket-name key]
  (let [bucket (.getBucket s3 bucket-name)
        obj (.getObject s3 bucket key)]
    (s3-deserialize
      (.getWrappedInputStream 
      (.getDataInputStream obj))
     (Object.))))

;;TODO: is there a shorter way to deal with the stream hell of
;;getting a java object serialized onto an input stream?
;;references
;;http://markmail.org/message/n5otqusrl6jda4ei
;;http://www.exampledepot.com/egs/java.io/serializeobj.html
(defn put-obj [s3 bucket-name key obj]
  (let [bos (ByteArrayOutputStream.)
	out (ObjectOutputStream. bos)
	_ (.writeObject out obj)
	_ (.close out)
	ba (.toByteArray out)
	bucket (.getBucket s3 bucket-name)  
        s3-object (S3Object. bucket key)
	_ (.setContentLength s3-object (.length ba)) 
	_ (.setDataInputStream s3-object (ByteArrayInputStream. ba))]
    (.putObject s3 bucket s3-object)))

(defn get-obj [s3 bucket-name key]
  (let [bucket (.getBucket s3 bucket-name)
        s3-obj (.getObject s3 bucket key)
	ois (ObjectInputStream.
	     (.getWrappedInputStream (.getDataInputStream s3-obj)))
	obj (.readObject ois)
	_  (.close ois)]
    obj))

;;TODO: the high throughput way is to do eventual appendes, keep writing and periodiucly roll up via some compactor service.
;;read in root and individual appends.
;;append
;;write out append to 3rd location.
;;move append to primary location
;;delete individual appends.
(defn append-str [s3 bucket-name key data]
  (let [old (get-str s3 bucket-name key)]
    (put-str s3 bucket-name key (append [old data]))))

(defn append-clj [s3 bucket-name key data]
  (let [old (get-clj s3 bucket-name key)]
    (put-clj s3 bucket-name key (append [old data]))))

(defn files [dir]
  (for [file (file-seq (File. dir))
	      :when (.isFile file)]
	     file))

;;TODO: refactor to use built in multi-file upload.
;;http://markmail.org/message/n5otqusrl6jda4ei
(defn put-dir
"create a new bucket b. copy everything from dir d to bucket b."
[s3 d b]
    (create-bucket s3 b)
    (dorun 
      (for [f (files d)]
	     (put-file s3 b f))))

(defn get-dir
"read the object(s) at the root/rest s3 uri into memory using a s3/get-foo fn."
 [s3 root-bucket rest rdr]
  (let [files (without-folders (objects s3 root-bucket rest))]
    (map #(rdr s3 root-bucket (.getKey %)) files)))