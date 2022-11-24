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