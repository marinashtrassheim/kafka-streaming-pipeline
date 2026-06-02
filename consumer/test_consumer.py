import asyncio
from clickhouse_connect import get_client


async def test_clickhouse_connection():
    """Test 1: ClickHouse доступен"""
    try:
        client = get_client(host='localhost', port=8123)
        result = client.query("SELECT 1")
        assert result.result_rows[0][0] == 1
        print("✅ ClickHouse connected")
        return True
    except Exception as e:
        print(f"❌ ClickHouse connection failed: {e}")
        return False


async def test_tables_exist():
    """Test 2: Таблицы созданы"""
    client = get_client(host='localhost', port=8123)

    tables = client.query("SHOW TABLES").result_rows
    table_names = [row[0] for row in tables]

    required = ['cart_events_raw', 'cart_events_1min']
    for table in required:
        if table in table_names:
            print(f"✅ Table {table} exists")
        else:
            print(f"❌ Table {table} missing")
            return False
    return True


async def test_kafka_connection():
    """Test 3: Kafka доступен"""
    from kafka import KafkaProducer
    try:
        producer = KafkaProducer(bootstrap_servers='localhost:9092')
        producer.close()
        print("✅ Kafka connected")
        return True
    except:
        print("❌ Kafka connection failed")
        return False


async def main():
    print("\n=== Testing Consumer Setup ===\n")

    results = await asyncio.gather(
        test_clickhouse_connection(),
        test_tables_exist(),
        test_kafka_connection()
    )

    print(f"\n=== Results: {sum(results)}/3 passed ===\n")


if __name__ == '__main__':
    asyncio.run(main())