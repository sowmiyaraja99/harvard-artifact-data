import streamlit as st
import requests
import json
import time
import pymysql
import pandas as pd

# ================= CONFIG =================
API_KEY = "38cb2140-dc22-41ab-95ec-74fc5517c88b"
BASE_URL = "https://api.harvardartmuseums.org/object"

RECORDS_REQUIRED = 2500
PAGE_SIZE = 100
PAGES = RECORDS_REQUIRED // PAGE_SIZE   # 25 pages = 2500 records

# ================= DB CONNECTION =================
def get_connection():
    return pymysql.connect(
        host="localhost",
        user="root",
        password="Yadhuyazhan2207",
        database="harvardproject",
        port=3306
    )

# ================= TABLE CREATION =================
def create_tables():
    conn = get_connection()
    cur = conn.cursor()

    cur.execute("""
    CREATE TABLE IF NOT EXISTS artifact_metadata (
        id INT PRIMARY KEY,
        title TEXT,
        culture TEXT,
        period TEXT,
        century TEXT,
        medium TEXT,
        department TEXT,
        classification TEXT
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS artifact_media (
        objectid INT PRIMARY KEY,
        imagecount INT,
        mediacount INT,
        colorcount INT,
        datebegin INT,
        dateend INT
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS artifact_colors (
        objectid INT,
        spectrum VARCHAR(50),
        hue VARCHAR(50),
        percent FLOAT,
        css3 VARCHAR(50)
    )
    """)

    conn.commit()
    conn.close()

# ================= FETCH DATA =================
def fetch_data(classification):
    metadata, media, colors = [], [], []

    progress = st.progress(0)
    status = st.empty()

    for page in range(1, PAGES + 1):
        status.text(f"Fetching page {page}/{PAGES}")

        params = {
            "apikey": API_KEY,
            "classification": classification,
            "size": PAGE_SIZE,
            "page": page
        }

        response = requests.get(BASE_URL, params=params, timeout=20)
        data = response.json()

        for r in data.get("records", []):

            metadata.append((
                r.get("id"),
                r.get("title"),
                r.get("culture"),
                r.get("period"),
                r.get("century"),
                r.get("medium"),
                r.get("department"),
                r.get("classification")
            ))

            media.append((
                r.get("objectid"),
                r.get("imagecount"),
                r.get("mediacount"),
                r.get("colorcount"),
                r.get("datebegin"),
                r.get("dateend")
            ))

            for c in r.get("colors", []):
                colors.append((
                    r.get("objectid"),
                    c.get("spectrum"),
                    c.get("hue"),
                    c.get("percent"),
                    c.get("css3")
                ))

        progress.progress(page / PAGES)
        time.sleep(0.2)

    status.success("✅ Data collection completed")
    return metadata, media, colors

# ================= INSERT DATA =================
def insert_data(metadata, media, colors):
    conn = get_connection()
    cur = conn.cursor()

    # ---- METADATA ----
    cur.executemany("""
    INSERT IGNORE INTO artifact_metadata
    (id, title, culture, period, century, medium, department, classification)
    VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
    """, metadata)

    # ---- MEDIA ----
    cur.executemany("""
    INSERT IGNORE INTO artifact_media
    (objectid, imagecount, mediacount, colorcount, datebegin, dateend)
    VALUES (%s,%s,%s,%s,%s,%s)
    """, media)

    # ---- COLORS ----
    cur.executemany("""
    INSERT INTO artifact_colors
    (objectid, spectrum, hue, percent, css3)
    VALUES (%s,%s,%s,%s,%s)
    """, colors)

    conn.commit()
    conn.close()


# ================= STREAMLIT UI =================
st.set_page_config("Harvard Art Explorer", layout="wide")

# -------- TITLE & INSTRUCTIONS --------
st.title("🎨 Harvard Art Museums Data Explorer")



# -------- CLASSIFICATION DROPDOWN --------
classification = st.selectbox(
    "Select Artifact Classification",
    ["Coins", "Paintings", "Sculpture"]
)

# -------- BUTTONS --------
col1, col2, col3 = st.columns(3)

with col1:
    collect_btn = st.button("📥 Collect Data")

with col2:
    show_btn = st.button("👁️ Show Data")

with col3:
    insert_btn = st.button("🗄️ Insert into SQL")

# -------- COLLECT DATA --------
if collect_btn:
    with st.spinner("Fetching data from Harvard API..."):
        meta, med, col = fetch_data(classification)
        st.session_state["data"] = (meta, med, col)
    st.success(f"Collected {len(meta)} records")

# -------- SHOW DATA --------
if show_btn:
    if "data" not in st.session_state:
        st.warning("Please collect data first")
    else:
        df = pd.DataFrame(
            st.session_state["data"][0],
            columns=[
                "id","title","culture","period",
                "century","medium","department","classification"
            ]
        )
        st.dataframe(df)

# -------- INSERT INTO SQL --------
if insert_btn:
    if "data" not in st.session_state:
        st.warning("Please collect data first")
    else:
        create_tables()
        insert_data(*st.session_state["data"])
        st.success("✅ Data inserted into MySQL")

# ================= QUERY & VISUALIZATION =================
st.divider()
st.subheader("📊 Query & Visualization Section")

query_option = st.selectbox(
    "Select Pre-written Query",
    [
       """SELECT *from artifact_metadata where century='11th century' and 
          culture='Byzantine culture'""",
          """select distinct culture from artifact_metadata""",
          """select *from artifact_metadata where period='Archaic Period'""",
          """select title,accessionyear from artifact_metadata order by accessionyear desc""",
          """select department,count(*) from artifact_metadata group by department""",
          """select objectid,imagecount from artifact_media where imagecount>1""",
          """select avg(`rank`) from artifact_media""",
          """select objectid from artifact_media where colorcount>mediacount""",
          """select *from artifact_media where datebegin >= 1500 and dateend<=1600""",
          """select *from artifact_media where mediacount=0""",
          """ select distinct hue from artifact_colors""",
          """SELECT color, COUNT(*) as frequency  FROM artifact_colors GROUP BY color
           ORDER BY frequency DESC LIMIT 5""",
        """SELECT hue, avg(percent) from artifact_colors GROUP BY hue ORDER BY hue""",
        """select count(*) from artifact_colors""",
        """select m.title,c.hue from artifact_metadata m join artifact_colors c on
          m.id=c.objectid where culture='Byzantine'""",
    
        """select m.title,c.hue from artifact_metadata as m join artifact_colors as c on
          m.id=c.objectid""",
        """select m.title,m.culture,media.rank from artifact_metadata as m join 
          artifact_media as media on m.id=media.objectid where m.period is not null""",
        """select distinct m.title from artifact_metadata as m join artifact_media as media on
          m.id=media.objectid join artifact_colors as c on m.id=c.objectid where media.`rank` <=10 
          and c.hue='grey'""",
        """select m.classification,count(*) as artifact_count,avg(media.mediacount) as avg_media_count
from artifact_metadata as m join artifact_media as media ON m.id = media.objectid GROUP BY m.classification""",
        """select objectid from artifact_media where mediacount=0 and colorcount=0""",
        """select objectid from artifact_media where datebegin=dateend""",
        """select *from artifact_metadata where century='20th century'""",
        """select objectid from artifact_media where colorcount>10""",
        """select id from artifact_metadata where medium='' 'Oil on canvas'"""
    ]
)

if st.button("▶ Run Query"):
    conn = get_connection()

    if query_option == "Artifacts count by classification":
        sql = """
        SELECT classification, COUNT(*) AS total
        FROM artifact_metadata
        GROUP BY classification
        """

    elif query_option == "Artifacts by century":
        sql = """
        SELECT century, COUNT(*) AS total
        FROM artifact_metadata
        GROUP BY century
        ORDER BY total DESC
        """

    elif query_option == "Top cultures":
        sql = """
        SELECT culture, COUNT(*) AS total
        FROM artifact_metadata
        GROUP BY culture
        ORDER BY total DESC
        LIMIT 10
        """

    else:
        sql = """
        SELECT m.id, m.title, md.colorcount
        FROM artifact_metadata m
        JOIN artifact_media md ON m.id = md.objectid
        ORDER BY md.colorcount DESC
        LIMIT 10
        """

    result = pd.read_sql(sql, conn)
    conn.close()

    st.dataframe(result)

    # Optional chart
    if st.checkbox("📈 Show Chart"):
        st.bar_chart(result.set_index(result.columns[0]))

# -------- FOOTER --------
st.divider()
st.caption("Data Source: Harvard Art Museums API (Educational Use)")
