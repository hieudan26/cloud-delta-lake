# Tìm hiểu Delta-lake và mô hình kết nối với AWS

Tên đề tài : Tìm hiểu Delta-lake và mô hình kết nối với AWS

## Yêu cầu : 
- Tìm hiểu Delta-lake và mô hình kết nối với AWS
- https://delta.io/

## Thành viên :       
- Nguyễn Hiếu Đan - 19110345
- Dương Đức Thắng - 19110461
- Ninh Phạm Trung Thành - 19110456
          
## Tìm hiểu về delta-lake :
### 1.1 Datalake là gì:
Data Lake (hay Hồ dữ liệu) là một kho lưu trữ tập trung được thiết kế để lưu trữ, xử lý và bảo mật một lượng lớn dữ liệu có cấu trúc, bán cấu trúc và phi cấu trúc. Nó có thể lưu trữ dữ liệu ở định dạng gốc và xử lý mọi loại dữ liệu khác nhau, bỏ qua các giới hạn về kích thước. Nó cung cấp số lượng dữ liệu cao để tăng hiệu suất phân tích và tích hợp gốc.
![image](https://user-images.githubusercontent.com/61266491/203808638-f30a0115-0510-45ca-b7a2-66cbc8ffe314.png)

### 1.2 Hadoop là gì:
Hadoop là một Apache framework mã nguồn mở cho phép phát triển các ứng dụng phân tán (distributed processing) để lưu trữ và quản lý các tập dữ liệu lớn. Hadoop hiện thực mô hình MapReduce, mô hình mà ứng dụng sẽ được chia nhỏ ra thành nhiều phân đoạn khác nhau được chạy song song trên nhiều node khác nhau. Hadoop được viết bằng Java tuy nhiên vẫn hỗ trợ C++, Python, Perl bằng cơ chế streaming.
Hadoop giải quyết:
- Xử lý và làm việc khối lượng dữ liệu khổng lồ tính bằng Petabyte.
- Xử lý trong môi trường phân tán, dữ liệu lưu trữ ở nhiều phần cứng khác nhau, yêu cầu xử lý đồng bộ
- Các lỗi xuất hiện thường xuyên.
- Băng thông giữa các phần cứng vật lý chứa dữ liệu phân tán có giới hạ
### 1.3 Spark là gì:
Apache Spark là một framework mã nguồn mở tính toán cụm, được phát triển sơ khởi vào năm 2009 bởi AMPLab. Sau này, Spark đã được trao cho Apache Software Foundation vào năm 2013 và được phát triển cho đến nay.

Tốc độ xử lý của Spark có được do việc tính toán được thực hiện cùng lúc trên nhiều máy khác nhau. Đồng thời việc tính toán được thực hiện ở bộ nhớ trong (in-memories) hay thực hiện hoàn toàn trên RAM.

Spark cho phép xử lý dữ liệu theo thời gian thực, vừa nhận dữ liệu từ các nguồn khác nhau đồng thời thực hiện ngay việc xử lý trên dữ liệu vừa nhận được ( Spark Streaming).

Spark không có hệ thống file của riêng mình, nó sử dụng hệ thống file khác như: HDFS, Cassandra, S3,…. Spark hỗ trợ nhiều kiểu định dạng file khác nhau (text, csv, json…) đồng thời nó hoàn toàn không phụ thuộc vào bất cứ một hệ thống file nào.
### 1.4 Delta-lake là gì:
Delta lake là một framework lưu trữ mã nguồn mở được ra mắt vào năm 2019 cho phép xây dựng kiến trúc datalake với các công cụ tính toán như  Spark, PrestoDB, Flink, Trino, Hive và các API sử dụng Scala, Java, Rust, Ruby, and Python
- Delta Lake là một lớp lưu trữ nguồn mở đảm bảo độ tin cậy cho các hồ dữ liệu.
- Nó được thiết kế đặc biệt để hoạt động với Databricks File System (DBFS) và Apache Spark.
- Nó cung cấp khả năng xử lý dữ liệu hàng loạt và phát trực tuyến hợp nhất, giao dịch ACID và xử lý siêu dữ liệu có thể mở rộng.
- Nó lưu trữ dữ liệu của bạn dưới dạng tệp Apache Parquet trong DBFS và duy trì nhật ký giao dịch theo dõi chính xác các thay đổi đối với bảng.
- Nó làm cho dữ liệu sẵn sàng để phân tích.
### 2. Tại sao phải dùng delta-lake:
 ![Logo TechMaster](https://miro.medium.com/max/1400/1*PWgtIlv53i9eExPuX6IpUQ.png)
- Hỗ trợ ACID transaction.
- Tận dụng sức mạnh xử lý phân tán của Spark để xử lý tất cả siêu dữ liệu cho các bảng quy mô hàng petabyte với hàng tỷ file một cách dễ dàng.
- Time travel - Dễ dàng truy cập và hoàn lại những phiên bản trước của dữ liệu.
- Định dạng mở lưu trữ dưới Parquet file.
- Hợp nhất Batch & Streaming, Source & Sink.
- Dễ dàng thay đổi lược đồ hiện tại của bảng để phù hợp với dữ liệu.
- Hiệu suất nhanh với Apache Spark.
- Hỗ trợ MERGE, UPDATE, DELETE và phát trực tuyến UPSERTS.
### 3. Kiến trúc của delta-lake:
Delta Lake thuộc Transaction Layer nằm trên đỉnh storage layer của data lake để nhận dữ liệu đáng tin cậy trong các hồ dữ liệu đám mây như Amazon S3 và ADLS Gen2 . 
Delta Lake đảm bảo dữ liệu nhất quán, đáng tin cậy với các giao dịch ACID, lập phiên bản dữ liệu tích hợp và kiểm soát để đọc và ghi đồng thời.
Nó còn cho phép tái tạo báo cáo dễ dàng và đáng tin cậy.</br></br>
Kiến trúc Delta Lake là một cải tiến lớn so với kiến trúc Lambda truyền thống.</br></br>
Ở mỗi giai đoạn, Delta-lake dữ liệu của mình thông qua một quy trình được kết nối cho phép chúng  kết hợp luồng công việc theo lô(Batch workflow) và luồng(Stream) thông qua kho lưu trữ tệp được chia sẻ với các giao dịch tuân thủ ACID.
</br></br>Delta lake sắp xếp dữ liệu của mình thành các lớp hoặc thư mục được xác định là Bronze, Silver và Gold như sau:
- Bảng Bronze có dữ liệu thô được nhập từ nhiều nguồn khác nhau (dữ liệu RDBMS, tệp JSON, dữ liệu IoT, v.v.).
- Các bảng Silver sẽ cung cấp một cái nhìn tinh tế hơn về dữ liệu của chúng tôi. Delta lake kết hợp các trường từ nhiều bảng Bronze khác nhau để cải thiện hồ sơ phát trực tuyến(streaming records) hoặc cập nhật trạng thái tài khoản dựa trên hoạt động gần đây.
- Bảng Gold cung cấp tổng hợp các bản báo cáo ở business-level thường được sử dụng để lập bảng điều khiển và báo cáo. Điều này sẽ bao gồm các tổng hợp như doanh số hàng tuần trên mỗi cửa hàng, người dùng trang web hoạt động hàng ngày hoặc tổng doanh thu mỗi quý của bộ phận.
Kết quả cuối cùng là thông tin chi tiết có thể hành động, bảng điều khiển và báo cáo về các số liệu kinh doanh.
 ![Logo TechMaster](https://k21academy.com/wp-content/uploads/2021/05/delta-lake-img.png)
### 4. Mô hình kết nối với AWS:
Để nhờ vào delta lake kết hợp với các dịch vụ AWS mà chúng sẽ sẽ xây dựng được 1 hệ thống lakehouse có đầy đủ tính năng hoàn chỉnh.
 ![Logo TechMaster](https://cms.databricks.com/sites/default/files/AWS-Data-Lake-Architecture-light-BG-1.png)
 Trong đó:
- S3 là nơi lưu trữ chính của các Batch data 
- Kinesis là nơi xử lý tiếp nhận dữ liệu thời gian thực theo luồng từ các dịch vụ khác của aws hoặc ngoài aws.
- Delta lake được deploy trên các nền tảng của AWS có thể kiểm soát đăng nhập bảo mật bằng IAM và Role
- Tiếp là Delta lake sẽ kết hợp cũng AWS glue để xử lý dữ liệu
- Ridshift và Athena cho phép truy vấn sắp xếp lại dữ liệu để đưa ra QuickSight thành những bản báo cáo và gửi đến người dùng cuối
### 5.Demo:
#### 5.1: Tổng quan phần thực hành:
- Cài đặt được spark và delta-lake trên instance EC2 kết nối với S3 và thực hiện các câu lệnh truy vấn, xử lý dữ liệu theo delta-lake framework bằng cách ssh vào EC2 instant
- Cấu hình EMR cho phép sử dụng delta-lake framework và thực hiện truy vấn dữ khai thác dữ liệu qua EMR notebook.
#### 5.2: Các bước cài đặt trên EC2 instance:
##### 5.2.1: Tạo EC2 instance
![image](https://user-images.githubusercontent.com/61266491/203777666-bac42b18-b981-485d-83f9-d86299b191b6.png)
Tạo 1 instance EC2 như bài thực hành lab đã học với cấu hình tối thiểu  2 vCPUs. Có thể xài bất cứ hệ điều hành gì ở đây em chọn ubuntu vì tính quen thuộc và ổn định.
##### 5.2.2: Tạo bucket S3

![image](https://user-images.githubusercontent.com/61266491/203778190-ebb61697-a030-4d32-bf8c-7656055babe9.png)
Tạo 1 bucket S3 để lưu trữ dữ liệu.
Lưu ý: Ở đây do tài khoản lab leaner không hỗ trợ IAM nên em public nó luôn để sử dụng.
##### 5.2.3: Truy cập vào EC2 instance

![image](https://user-images.githubusercontent.com/61266491/203778454-03b240dc-f73c-4eac-a170-4370720872b8.png)
Kết nối trực tiếp đến EC2 instance để dụng.
Có thể thông qua SSH để truy cập vào vì nhanh và đơn giản em truy cập trực tiếp ở website aws
##### 5.2.4: Cài đặt python và java

![image](https://user-images.githubusercontent.com/61266491/203778704-1d0cc332-a42f-4929-b2c5-b7e8a45830c9.png)
Sử dụng lệnh để cài đặt java và python.
Yêu cầu cài đặt Python > 3.8 và jdk > 11
Sau đó cài pip3 để cài đặt package cần sử dụng
```bat
python3 –verion
java –version
apt-get update
apt install openjdk-11-jre-headless
sudo apt install python3-pip
```
##### 5.2.5: Cài đặt PySpark
![image](https://user-images.githubusercontent.com/61266491/203779518-9b5dbe0a-c071-4063-b68b-350a9c9b025c.png)
```bat
pip install pyspark==3.2.2
```
##### 5.2.6: Cấu hình cài đặt PySpark và Delta-lake
![image](https://user-images.githubusercontent.com/61266491/203779795-8d0c3cce-46b2-4b28-ab56-36d33626fdcc.png)
```python
from pyspark.sql import SparkSession
spark_jars_packages = "com.amazonaws:aws-java-sdk:1.12.246,org.apache.hadoop:hadoop-aws:3.2.2,io.delta:delta-core_2.12:1.2.1,"
spark = (
            SparkSession.builder.master("local[*]")
            .appName("PySparkLocal") # tên app
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") # phần extension sql sẽ là deltasparksession
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") #set catalog là thư viện delta
            .config("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")  # sử dụng s3a hỗ trợ lên tới tệp 5TB
            .config("spark.hadoop.fs.AbstractFileSystem.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") # tương tự
            .config("spark.delta.logStore.class", "org.apache.spark.sql.delta.storage.S3SingleDriverLogStore") # lưu log tại S3
            .config("spark.hadoop.fs.s3a.connection.timeout", "3600000") #time out
            .config("spark.hadoop.fs.s3a.connection.maximum", "1000") # connection tối đa
            .config("spark.hadoop.fs.s3a.threads.max", "1000") # số thread tối đa
            .config("spark.jars.packages", spark_jars_packages) # các jars package được khai báo wor trên
            .config("spark.sql.sources.partitionOverwriteMode", "dynamic") # Ghi đè các phân vùng
            .config("spark.databricks.delta.schema.autoMerge.enabled", "true") # tự động merge
            .config("spark.hadoop.fs.s3a.endpoint", "s3.us-east-1.amazonaws.com") # đầu cuối s3
            .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider") #cung cấp credentials
            .getOrCreate()
        )
```
Dòng đầu tiên là import thử viện</br>
Dòng thứ 2 khải báo các thư viện core để sử dụng</br>
Dòng thứ 3 đến cuối là confuration cấu hình và build SparkSession</br>
![image](https://user-images.githubusercontent.com/61266491/203783425-f90bca54-b4c9-4b6e-9f16-593f0c03aa01.png)
##### 5.2.7: Import dữ liệu vào S3
![image](https://user-images.githubusercontent.com/61266491/203784523-7ccbf510-7cec-4745-b004-22c80b8b551c.png)
Import dữ liệu từ: https://stats.govt.nz/large-datasets/csv-files-for-download/
##### 5.2.8: Đọc thử dữ liệu từ S3
 ![image](https://user-images.githubusercontent.com/61266491/203784748-f8e9a58b-baeb-473c-a116-3c8ed4721b30.png)
 Đọc file csv với header bằng lệnh: 
 ```python
 df = spark.read.option("recursiveFileLookup", "true").option("header","true").csv("s3://delta-lake-ute/machine-readable-business-employment-data-mar-2022-quarter.csv")
 ```
Sau đó thực hiện tạo một view tạm và thực hiện câu truy vấn:
```python
spark.sql("select Series_title_2,count(*) as count from employment_tbl group by Series_title_2 order by 2 desc").show(truncate=False) 
```
mục đích của câu lệnh này là lấy ra các tile và đếm số lượng của nhân viên trong title đó từ file csv
##### 5.2.8: Kết quả
![image](https://user-images.githubusercontent.com/61266491/203785192-fefdf644-6f22-4a7e-aaf7-ba7edccbfeee.png)
Sử dụng các câu lệnh sau bằng spark và delta lake:  
Tạo data frame
```python
df_groupby = spark.sql("select Series_title_2,count(*) as count from employment_tbl group by Series_title_2 order by 2 desc")
```
Lưu data frame dưới dạng delta format
```python
df_groupby.write.format("delta").save("s3://delta-lake-ute/sample_data/")
```
Đọc data delta format 
```python
df = spark.read.format("delta").load("s3://delta-lake-ute/sample_data/")
```
Show ra data đã đọc 
```python
df.show()
```
Kiểm tra S3 ta thấy file có phần mở rộng là panquet đó là dạng file của delta làm việc 
Dạng file này sẽ cho tốc độ truy vấn nhanh gấp nhiều lần csv
#### 5.3: Cấu hình trên EMR
#### 5.3.1: Tạo bucket S3
![image](https://user-images.githubusercontent.com/61266491/203788660-51c7a167-28d4-46ca-b177-aac18661ede8.png)

</br>Ute-delta-lake: dùng để lưu dữ liệu khi làm việc</br>
Ute-erm-boostrap: dùng để lưu file cho việc bootstrap của emr service</br>
Tạo 1 file với tên là deltajarinstall.sh với nội dung:
```bat
#!/bin/bash
sudo curl -O --output-dir /usr/lib/spark/jars/  https://repo1.maven.org/maven2/io/delta/delta-core_2.12/2.0.0/delta-core_2.12-2.0.0.jar
sudo curl -O --output-dir /usr/lib/spark/jars/  https://repo1.maven.org/maven2/io/delta/delta-storage/2.0.0/delta-storage-2.0.0.jar
sudo python3 -m pip install delta-spark==2.0.0
```
#### 5.3.2.1: Tạo EMR instace (cloud shell)
![image](https://user-images.githubusercontent.com/61266491/203790348-c77e0009-4e28-41dc-bb28-be191171fb5b.png)

</br>Ở dịch vụ cloud shell nhập lệnh sau để tạo emr install với pyspark core là delta lake:
```bat
aws emr create-cluster \
--name "emr-delta-lake-blog" \
--release-label emr-6.7.0 \
--applications Name=Hadoop Name=Hive Name=Livy Name=Spark Name=JupyterEnterpriseGateway \
--instance-type m5.xlarge \
--instance-count 1 \
--ec2-attributes SubnetId='subnet-0017deaed524d4e72' \
--use-default-roles \
--bootstrap-actions Path="s3://ute-erm-boostrap/deltajarinstall.sh"
```
#### 5.3.2.2: Tạo EMR thủ công 
![image](https://user-images.githubusercontent.com/61266491/203790661-6c1fee18-f8dd-4dd4-ad07-49ff70cd33ab.png)
![image](https://user-images.githubusercontent.com/61266491/203790785-96382a3e-647c-469e-a3ba-9185521a25a8.png)
![image](https://user-images.githubusercontent.com/61266491/203790815-d4193d1d-b3b0-4164-a11d-2cfa3400155f.png)
![image](https://user-images.githubusercontent.com/61266491/203790852-41e0df86-f639-4a5d-a7d3-dd9df5a8a87d.png)
</br>Chọn cấu hình như hình.
#### 5.3.3: Kiểm tra xem EMR instace tạo thành công
![image](https://user-images.githubusercontent.com/61266491/203790900-696111e2-1619-427f-bd56-be38a48fab1e.png)
#### 5.3.4: Tạo notebook
![image](https://user-images.githubusercontent.com/61266491/203791163-c38073b3-2658-478a-b31c-18d87be843da.png)
![image](https://user-images.githubusercontent.com/61266491/203791334-a3d842fb-1c77-45c2-92fb-9fbc32ab938e.png)
#### 5.3.5: Sử dụng NoteBook để khai thác dữ liệu
![image](https://user-images.githubusercontent.com/61266491/203792250-2c1b13df-b99d-48a6-9f05-e9980cc563f7.png)
</br>Mở notebook và chọn PySpark Kernel
#### 5.3.6: Configuration cho PySpark
![image](https://user-images.githubusercontent.com/61266491/203792949-e105ad39-cd00-4eb1-be19-33d47c584ae5.png)
</br>Cài đặt chạy apache spark với delta core
Sau đó import thư viện dùng để chạy các lệnh truy vấn delta
```python
%%configure -f
{
  "conf": {
    "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
    "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog"
  }
}
from delta.tables import *
from pyspark.sql.functions import *
```
![image](https://user-images.githubusercontent.com/61266491/203793481-cdb00068-211c-48f0-87fd-2769291e2143.png)

</br>Sử dụng bucket được public của aws(Amazon Product Reviews Dataset) để đọc và viết data delta
https://us-east-1.console.aws.amazon.com/s3/home?region=us-east-1&bucket=amazon-reviews-pds

![image](https://user-images.githubusercontent.com/61266491/203793668-1690ffeb-5247-4aab-b52d-94160f35954a.png)
</br>Set deltaPath là vị trí s3 bucket được tạo ởbước 1
Kiểm tra spark đọc file banquet từ public bucket:
```python
deltaPath = "s3://ute-delta-lake/delta-amazon-reviews-pds/"
df_parquet = spark.read.parquet("s3://amazon-reviews-pds/parquet/product_category=Gift_Card/*.parquet"
df_parquet.printSchema()
```

![image](https://user-images.githubusercontent.com/61266491/203793857-07901a79-22df-4fd6-b2dd-53eb05c41cf8.png)
</br>Tiến hành lưu các file theo định dạng delta xuống bucket S3 đã tạo ở bước 1
Kiểm tra thấy lưu thành công nghĩa là việc cài đặt đã hoàn tất thành công.
```python
df_parquet.write.mode("overwrite").format("delta").partitionBy("year").save(deltaPath)
```

![image](https://user-images.githubusercontent.com/61266491/203794075-6359bdd1-2f71-4b3b-8e5f-16bf98a3cad6.png)
</br>Đọc và show lên lại file vừa lưu:
```python
df_delta = spark.read.format("delta").load(deltaPath)
df_delta.show()
```
### 5.4: Khai thác dữ liệu bằng delta-lake trên notebook
##### 5.4.1: Chuẩn bị dữ liệu
![image](https://user-images.githubusercontent.com/61266491/203794460-6ca20efd-0e5a-4036-88c4-3dd2578dadcd.png)
</br> Sử dụng lại kho dữ liệu của amazone customer review dataset được quyền sủ dụng cho mục đích học thuật.
https://us-east-1.console.aws.amazon.com/s3/home?region=us-east-1&bucket=amazon-reviews-pds
![image](https://user-images.githubusercontent.com/61266491/203794586-1b665016-5133-4459-8c4f-05b2d6824d18.png)
</br>Sau đó chúng ta đọc toàn bộ file .parquet từ danh mục Gift_Card của dữ liệu aws public và lưu vào df_parquet
Sau đó chúng ta sử dụng SQL để lưu về bucket đã chuẩn bị từ trước của chúng ta theo từng năm 1
```python
deltaPath = "s3://ute-delta-lake/delta-amazon-reviews-pds/"
df_parquet = spark.read.parquet("s3://amazon-reviews-pds/parquet/product_category=Gift_Card/*.parquet")
df_parquet.printSchema()
df_parquet.write.mode("overwrite").format("delta").partitionBy("year").save(deltaPath)
spark.conf.set('table.location', deltaPath)
```
##### 5.4.2: Đọc dữ liệu
![image](https://user-images.githubusercontent.com/61266491/203795261-fcee882e-a31c-4077-83ce-3fdd5072c6ee.png)
![image](https://user-images.githubusercontent.com/61266491/203801918-18383f43-52d9-4e7c-b0f3-ecb672b1ef32.png)
![image](https://user-images.githubusercontent.com/61266491/203801955-b2f2f779-4796-4001-8081-f85ff6bc92c8.png)

Dùng spark để đọc dữ liệu lên
Dùng lệnh show để tiến hành show ra dữ liệu đã đọc
```python
df_delta = spark.read.format("delta").load(deltaPath)
df_delta.show()

%%sql
SELECT * FROM  delta.`s3://ute-delta-lake/delta-amazon-reviews-pds/` LIMIT 10

df_delta.createOrReplaceTempView("aws_product_review")
spark.sql("select marketplace,customer_id,review_date  from aws_product_review LIMIT 30").show(30)
```
##### 5.4.3: Cập nhật dữ liệu
![image](https://user-images.githubusercontent.com/61266491/203802073-fadff51e-aa2d-4518-a47b-6373255ab148.png)
</br>Chuyển US sang USA
```python
deltaTable = DeltaTable.forPath(spark, deltaPath) #khai báo 1 datatable
deltaTable.update("marketplace = 'US'",{ "marketplace":"'USA'"}) # Thay đổi toàn bộ marketplace US thành USA
#hoặc
%%sql
update delta.`s3://ute-delta-lake/delta-amazon-reviews-pds/` 
set marketplace = 'USA' where marketplace = 'US'
```
##### 5.4.4: Xóa dữ liệu
![image](https://user-images.githubusercontent.com/61266491/203802518-8216d8f9-6081-4fdd-b187-3c6061f54d7d.png)
```python
df_delta.filter("verified_purchase = 'N'").show() #Xem dữ liệu trước khi xóa
deltaTable.delete("verified_purchase = 'N'") #Xóa dữ liệu
df_delta.filter("verified_purchase = 'N'").show() #Show lại sau khi xóa
```
![image](https://user-images.githubusercontent.com/61266491/203802619-1bc6d9a1-a862-4a8a-b28b-ac344fd411a3.png)
</br>Lưu ý rằng phương thức xóa chỉ loại bỏ dữ liệu khỏi phiên bản mới nhất của bảng. Những hồ sơ này vẫn có mặt trong các snap shot cũ hơn của dữ liệu.
```python
prev_version = deltaTable.history().selectExpr('max(version)').collect()[0][0] - 1
prev_version_data = spark.read.format('delta').option('versionAsOf', prev_version).load(deltaPath)
prev_version_data.filter("verified_purchase = 'N'").show(10)
```
##### 5.4.5: Time travel
![image](https://user-images.githubusercontent.com/61266491/203803821-06338f93-2600-4175-8234-c36ab1aa20d5.png)
```python
deltaTable.history(100).select("version", "timestamp", "operation", "operationParameters").show(truncate=False) # show ra lịch sử chỉnh sửa
df_time_travel = spark.read.format("delta").option("versionAsOf", 0).load(deltaPath) # đọc lại verion 0 ở lịch sử chỉnh sửa và show ra ta có thể thấy là marketplace vẫn là US trước khi sửa
df_time_travel.show()
```
##### 5.4.5: Upsert
![image](https://user-images.githubusercontent.com/61266491/203804279-dc57dac4-20e8-4ba4-8bbc-ec469c8a6c35.png)
</br>Tạo ra 1 list gồm 2 item để thực hiện upsert update XODE và insert XOA1 vì nó chưa tồn tại
Sau đó tạo 1 frame chứa data để upsert
```python
spark.sql("select * from aws_product_review where review_id in ('R315TR7JY5XODE', 'R315TR7JY5XOA1')").show() # show kết quả trước khi upsert
data_upsert = [ {"marketplace":'US',"customer_id":'38602100', "review_id":'R315TR7JY5XODE',"product_id":'B00CHSWG6O',"product_parent":'336289302',"product_title" :'Amazon eGift Card', "star_rating":'5', "helpful_votes":'2',"total_votes":'0',"vine":'N',"verified_purchase":'Y',"review_headline":'GREAT',"review_body":'GOOD PRODUCT',"review_date":'2014-04-11',"year":'2014'},
{"marketplace":'US',"customer_id":'38602103', "review_id":'R315TR7JY5XOA1',"product_id":"B007V6EVY2","product_parent":'910961751',"product_title" :'Amazon eGift Card', "star_rating":'5', "helpful_votes":'2',"total_votes":'0',"vine":'N',"verified_purchase":'Y',"review_headline":'AWESOME',"review_body":'GREAT PRODUCT',"review_date":'2014-04-11',"year":'2014'}
]
df_data_upsert = spark.createDataFrame(data_upsert)
df_data_upsert.show()
```
![image](https://user-images.githubusercontent.com/61266491/203804332-d0f54653-5e9b-4ca4-a615-739d6430756a.png)
```python
(deltaTable
.alias('t')
.merge(df_data_upsert.alias('u'), 't.review_id = u.review_id')
.whenMatchedUpdateAll()
.whenNotMatchedInsertAll()
.execute())
spark.sql("select * from aws_product_review where review_id in ('R315TR7JY5XODE', 'R315TR7JY5XOA1')").show() # show sau khi khi upsert
```
# Tài liệu tham khảo:
https://towardsdatascience.com/getting-started-with-delta-lake-spark-in-aws-the-easy-way-9215f2970c58
https://aws.amazon.com/vi/blogs/big-data/build-a-high-performance-transactional-data-lake-using-open-source-delta-lake-on-amazon-emr/
https://garystafford.medium.com/building-a-simple-data-lake-on-aws-df21ca092e32