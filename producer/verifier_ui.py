import streamlit as st
import json
import uuid
import pandas as pd
from confluent_kafka import Consumer, KafkaError
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import SerializationContext, MessageField
from minio import Minio

# --- CONFIGURATION ---
KAFKA_TOPIC = "users"
BOOTSTRAP_SERVERS = 'localhost:9092'
SCHEMA_REGISTRY_URL = 'http://localhost:8081'
MINIO_ENDPOINT = "localhost:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
BUCKET_NAME = "user-profiles-dump"

# --- HELPER FUNCTIONS ---

def get_kafka_schema():
    """Fetches schema from Registry."""
    try:
        sr_client = SchemaRegistryClient({'url': SCHEMA_REGISTRY_URL})
        schema = sr_client.get_latest_version(f"{KAFKA_TOPIC}-value")
        return json.loads(schema.schema.schema_str)
    except Exception as e:
        return {"error": str(e)}

def get_kafka_data():
    """Reads messages from Kafka (Earliest)."""
    sr_client = SchemaRegistryClient({'url': SCHEMA_REGISTRY_URL})
    avro_deserializer = AvroDeserializer(sr_client)

    conf = {
        'bootstrap.servers': BOOTSTRAP_SERVERS,
        'group.id': f"verifier-ui-{uuid.uuid4()}",
        'auto.offset.reset': 'earliest'
    }
    consumer = Consumer(conf)
    consumer.subscribe([KAFKA_TOPIC])

    results = []
    empty_polls = 0

    progress_bar = st.progress(0)
    status_text = st.empty()
    status_text.text("Connecting to Kafka...")

    try:
        # Poll for up to 3 seconds or until data stops coming
        for i in range(30):
            msg = consumer.poll(0.5)
            progress_bar.progress((i + 1) / 30)

            if msg is None:
                empty_polls += 1
                if empty_polls > 2: break
                continue

            if msg.error():
                continue

            try:
                user_data = avro_deserializer(msg.value(), SerializationContext(msg.topic(), MessageField.VALUE))
                results.append(user_data)
                empty_polls = 0
            except Exception:
                pass
    finally:
        consumer.close()
        progress_bar.empty()
        status_text.empty()

    return results

def get_minio_data():
    """Reads JSON files from MinIO."""
    client = Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False
    )
    results = []
    try:
        objects = client.list_objects(BUCKET_NAME, recursive=True)
        for obj in objects:
            data = client.get_object(BUCKET_NAME, obj.object_name)
            content = data.read().decode('utf-8')
            for line in content.split('\n'):
                if line.strip():
                    try:
                        results.append(json.loads(line))
                    except: pass
    except Exception as e:
        st.error(f"MinIO Error: {e}")
    return results

def run_comparison(kafka_data, minio_data):
    """Compares datasets and returns a DataFrame."""
    source_map = {u['user_id']: u for u in kafka_data}
    sink_map = {u['user_id']: u for u in minio_data}
    all_ids = sorted(set(source_map.keys()) | set(sink_map.keys()))

    report_data = []

    for uid in all_ids:
        src = source_map.get(uid)
        sink = sink_map.get(uid)

        status = "✅ OK"
        details = "Matched"

        if src and not sink:
            if src.get('first_name') == "":
                status = "🚫 FILTERED"
                details = "Empty name (Expected)"
            else:
                status = "❌ MISSING"
                details = "Not found in Sink"
        elif not src and sink:
            status = "❓ ORPHAN"
            details = "Found in Sink only"
        elif sink and 'raw_password_hash' in sink:
            status = "❌ FAILED"
            details = "Password hash not removed!"

        report_data.append({
            "User ID": uid,
            "Kafka (Source)": "Found" if src else "Missing",
            "MinIO (Sink)": "Found" if sink else "Missing",
            "Status": status,
            "Details": details
        })

    return pd.DataFrame(report_data)

# --- STREAMLIT UI ---

st.set_page_config(page_title="Pipeline Verifier", layout="wide", page_icon="🔍")

st.title("🔍 Data Pipeline Verifier")
st.markdown(f"**Source:** Kafka (`{KAFKA_TOPIC}`) ➡ **Transform** ➡ **Sink:** MinIO (`{BUCKET_NAME}`)")

if st.button("🔄 Run Verification", type="primary"):
    with st.spinner("Fetching data from Kafka and MinIO..."):
        kafka_data = get_kafka_data()
        minio_data = get_minio_data()

    # --- METRICS ---
    col1, col2, col3 = st.columns(3)
    col1.metric("Kafka Records", len(kafka_data))
    col2.metric("MinIO Records", len(minio_data))

    match_count = len(kafka_data) == len(minio_data)
    col3.metric("Count Match", "Yes" if match_count else "No", delta_color="normal" if match_count else "inverse")

    # --- TABS ---
    tab1, tab2, tab3 = st.tabs(["📊 Comparison Report", "📜 Schemas", "💾 Raw Data Explorer"])

    with tab1:
        st.subheader("Row-by-Row Verification")
        if kafka_data or minio_data:
            df = run_comparison(kafka_data, minio_data)

            # Highlight errors
            def highlight_status(val):
                color = 'red' if '❌' in val else 'green' if '✅' in val else 'orange' if '🚫' in val else 'black'
                return f'color: {color}; font-weight: bold'

            st.dataframe(df.style.applymap(highlight_status, subset=['Status']), use_container_width=True)
        else:
            st.warning("No data found in either system.")

    with tab2:
        col_a, col_b = st.columns(2)
        with col_a:
            st.markdown("### Kafka Avro Schema")
            st.json(get_kafka_schema())
        with col_b:
            st.markdown("### MinIO JSON Structure (Inferred)")
            if minio_data:
                st.json(minio_data[0])
            else:
                st.info("No MinIO data to infer schema.")

    with tab3:
        st.subheader("Kafka Source Data")
        st.dataframe(pd.DataFrame(kafka_data), use_container_width=True)

        st.divider()  # Adds a nice visual line between the tables

        st.subheader("MinIO Sink Data")
        st.dataframe(pd.DataFrame(minio_data), use_container_width=True)

else:
    st.info("Click 'Run Verification' to start.")