from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream import SimpleStringSchema
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.connectors import KafkaSource, JDBCAppendTableSink
from pyflink.common import Types
from pyspark.sql import Row
import json
import psycopg2
from psycopg2 import sql

def process_message(message):
    """Giải mã JSON và tính toán các phép tính cần thiết"""
    transaction = json.loads(message)
    # Lấy dữ liệu từ message
    item_code = transaction["ItemCode"]
    items_purchased = transaction["NumberOfItemsPurchased"]
    cost_per_item = transaction["CostPerItem"]
    item_description = transaction["ItemDescription"]
    
    # Tính tổng doanh thu cho giao dịch này (Doanh thu = số lượng * giá mỗi sản phẩm)
    total_revenue = items_purchased * cost_per_item
    
    # Trả về tuple chứa các giá trị cần thiết
    return (item_code, items_purchased, total_revenue, item_description)

def main():
    # 1. Set up Flink execution environment
    env = StreamExecutionEnvironment.get_execution_environment()

    # 2. Cấu hình Kafka source
    kafka_properties = {
        "bootstrap.servers": "localhost:9092",
        "group.id": "flink-consumer",
    }

    kafka_source = KafkaSource.builder() \
        .set_bootstrap_servers("localhost:9092") \
        .set_group_id("flink-consumer") \
        .set_topics("transaction_test") \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .build()

    # 3. Tạo stream dữ liệu từ Kafka
    stream = env.from_source(kafka_source, Types.STRING(), "Kafka Source")

    # 4. Xử lý stream: tính toán tổng doanh thu và tổng số lượng
    processed_stream = stream.map(process_message, Types.TUPLE([Types.INT(), Types.INT(), Types.FLOAT(), Types.STRING()]))

    # 5. Tính toán tổng số lượng bán ra và doanh thu theo từng ItemCode
    result_stream = processed_stream \
        .key_by(lambda x: x[0]).reduce(lambda a, b: (
            a[0],  # ItemCode remains the same
            a[1] + b[1],  # Tổng số lượng sản phẩm bán ra
            a[2] + b[2],  # Tổng doanh thu
            a[3]  # ItemDescription remains the same
        ))

    # 6. Gửi kết quả tính toán vào PostgreSQL
    def jdbc_sink(row):
        # Kết nối đến PostgreSQL
        conn = psycopg2.connect(
            dbname="retail_db", user="retail_user", password="retail_password", host="localhost", port="5432"
        )
        cursor = conn.cursor()
        
        # Thực thi câu lệnh INSERT để lưu kết quả vào PostgreSQL
        insert_query = sql.SQL("""
            INSERT INTO revenue (item_code, total_quantity, total_revenue, item_description)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (item_code) DO UPDATE 
            SET total_quantity = revenue.total_quantity + EXCLUDED.total_quantity,
                total_revenue = revenue.total_revenue + EXCLUDED.total_revenue;
        """)
        cursor.execute(insert_query, row)
        
        conn.commit()
        cursor.close()
        conn.close()

    # Gửi kết quả vào PostgreSQL thông qua sink
    result_stream.add_sink(jdbc_sink)

    # 7. Thực thi job Flink
    env.execute("Kafka to PostgreSQL Flink Job")

if __name__ == "__main__":
    main()