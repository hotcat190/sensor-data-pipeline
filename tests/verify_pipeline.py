#!/usr/bin/env python3
import json
import sys
import time
from kafka import KafkaConsumer
import clickhouse_connect

# Cấu hình kết nối hệ thống (Mapping từ docker-compose)
KAFKA_BROKER = 'localhost:9092'  # Truy cập từ máy Host
CLICKHOUSE_HOST = 'localhost'
CLICKHOUSE_PORT = 8123
CLICKHOUSE_USER = 'default'
CLICKHOUSE_PASSWORD = ''

TOPICS = {
    'bme280': 'sensor_bme280_topic',
    'sds011': 'sensor_sds011_topic'
}

def print_header(title):
    print("\n" + "="*60)
    print(f" {title} ".center(60, "#"))
    print("="*60)

def verify_kafka_payloads():
    print_header("TÁC VỤ 1: ĐO ĐẠC & KIỂM TRA BẢN TIN TRÊN KAFKA")
    
    for sensor_name, topic in TOPICS.items():
        print(f"\n[*] Đang kết nối tới topic: '{topic}'...")
        try:
            consumer = KafkaConsumer(
                topic,
                bootstrap_servers=[KAFKA_BROKER],
                auto_offset_reset='earliest',
                enable_auto_commit=False,
                consumer_timeout_ms=3000, # Tránh treo script nếu hàng đợi trống
                value_deserializer=lambda x: json.loads(x.decode('utf-8'))
            )
            
            # Lấy bản tin đầu tiên để verify cấu trúc hình học dữ liệu (Schema verification)
            msg = next(consumer, None)
            if msg:
                print(f"[SUCCESS] Tìm thấy bản tin hợp lệ từ NiFi trên Kafka!")
                print(f"[-] Partition: {msg.partition} | Offset: {msg.offset}")
                print(f"[-] Nội dung JSON mẫu:")
                print(json.dumps(msg.value, indent=4, ensure_ascii=False))
                
                # Khảo sát nhanh các trường bắt buộc (Data Validation Rules)
                required_fields = ['sensor_id', 'sensor_type', 'timestamp', 'lat', 'lon']
                missing = [f for f in required_fields if f not in msg.value]
                if not missing:
                    print(f"[OK] Cấu trúc bản tin đầy đủ các trường Core Metrics.")
                else:
                    print(f"[WARNING] Bản tin bị thiếu trường bắt buộc: {missing}")
            else:
                print(f"[INFO] Chưa nhận được bản tin nào mới trên topic '{topic}' (Hàng đợi trống hoặc NiFi chưa đẩy dữ liệu).")
            
            consumer.close()
        except Exception as e:
            print(f"[ERROR] Lỗi khi kết nối hoặc đọc dữ liệu từ Kafka: {e}")

def verify_clickhouse_data():
    print_header("TÁC VỤ 2: KIỂM TRA SỐ LIỆU VÀ KIỂU DỮ LIỆU CLICKHOUSE")
    
    try:
        client = clickhouse_connect.get_client(
            host=CLICKHOUSE_HOST, 
            port=CLICKHOUSE_PORT, 
            username=CLICKHOUSE_USER, 
            password=CLICKHOUSE_PASSWORD
        )
        
        tables = [
            'sensor_storage.bme280_data',
            'sensor_storage.sds011_data'
        ]
        
        for table in tables:
            # 1. Kiểm tra tổng số dòng đã nạp
            count_res = client.query(f"SELECT count() FROM {table}")
            total_rows = count_res.first_row[0]
            print(f"\n[*] Bảng '{table}': Tổng số bản ghi hiện tại = {total_rows:,} dòng.")
            
            if total_rows > 0:
                print(f"[SUCCESS] Dữ liệu đã được nạp thành công thông qua Materialized View.")
                # 2. Xem thử cấu trúc thực tế của một bản ghi mới nhất
                sample_res = client.query(f"SELECT * FROM {table} ORDER BY timestamp DESC LIMIT 1")
                print(f"[-] Bản ghi mới nhất được lưu trữ:")
                for col_name, val in zip(sample_res.column_names, sample_res.first_row):
                    print(f"    |-- {col_name}: {val} ({type(val).__name__})")
            else:
                print(f"[WARNING] Bảng đang trống. Hãy kiểm tra lại trạng thái hoạt động của NiFi hoặc Kafka Consumer trong ClickHouse.")
                
    except Exception as e:
        print(f"[ERROR] Thất bại khi kết nối hoặc truy vấn ClickHouse: {e}")

def verify_compression_performance():
    print_header("TÁC VỤ 3: PHÂN TÍCH HIỆU QUẢ NÉN CỘT (CODEC ANALYSIS)")
    
    try:
        client = clickhouse_connect.get_client(
            host=CLICKHOUSE_HOST, 
            port=CLICKHOUSE_PORT, 
            username=CLICKHOUSE_USER, 
            password=CLICKHOUSE_PASSWORD
        )
        
        # Câu lệnh truy vấn metadata từ hệ thống để tính dung lượng nén cho từng cột đặc thù
        codec_query = """
            SELECT 
                table,
                column,
                type,
                formatReadableSize(sum(data_uncompressed_bytes)) AS uncompressed_size,
                formatReadableSize(sum(data_compressed_bytes)) AS compressed_size,
                round(sum(data_uncompressed_bytes) / sum(data_compressed_bytes), 2) AS compression_ratio
            FROM system.parts_columns
            WHERE database = 'sensor_storage' AND active = 1
              AND column IN ('temperature', 'humidity', 'pressure', 'P1', 'P2')
            GROUP BY table, column, type
            ORDER BY table, column
        """.strip()
        
        print("\n[*] Đang truy vấn hiệu năng nén từ `system.parts_columns`...")
        res = client.query(codec_query)
        
        if res.result_rows:
            print(f"\n{'BẢNG':<28} | {'CỘT':<13} | {'KIỂU SỐ':<10} | {'CHƯA NÉN':<10} | {'ĐÃ NÉN':<10} | {'TỶ LỆ NÉN'}")
            print("-" * 90)
            for row in res.result_rows:
                print(f"{row[0]:<28} | {row[1]:<13} | {row[2]:<10} | {row[3]:<10} | {row[4]:<10} | {row[5]}x")
            print("\n[INFO] Giải thích: Tỷ lệ nén (Compression Ratio) càng lớn thể hiện Codec (DoubleDelta/Gorilla) hoạt động càng hiệu quả.")
        else:
            print("\n[INFO] Chưa thu thập được số liệu nén. Cần chạy luồng ghi lượng lớn dữ liệu (Kịch bản 2) để ClickHouse thực hiện Merge Parts và ghi nhận thống kê.")
            
    except Exception as e:
        print(f"[ERROR] Không thể lấy báo cáo nén dữ liệu từ hệ thống: {e}")

if __name__ == '__main__':
    print("=== PIPELINE INTEGRATION & VERIFICATION PIPELINE (TDD-like) ===")
    start_time = time.time()
    
    verify_kafka_payloads()
    verify_clickhouse_data()
    verify_compression_performance()
    
    print(f"\n[✔] Quá trình kiểm thử tích hợp hoàn tất sau {time.time() - start_time:.2f} giây.\n")