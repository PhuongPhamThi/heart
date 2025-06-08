import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from pymongo import MongoClient
import uuid
from datetime import datetime

# Kết nối MongoDB
client = MongoClient('mongodb://localhost:27017/')
db = client['heart_disease_db']

# Xóa collection cũ (nếu có) và tạo mới
db.patients.drop()
db.health_indicators.drop()
db.lifestyle.drop()

# Đọc dữ liệu từ file CSV và giới hạn 1000 bản ghi
data = pd.read_csv('heart_2020_cleaned.csv').head(1500)

# Tạo 3 collection và nhập dữ liệu
patients_collection = db['patients']
health_indicators_collection = db['health_indicators']
lifestyle_collection = db['lifestyle']

for index, row in data.iterrows():
    patient_id = str(uuid.uuid4())
    
    # Collection patients
    patients_collection.insert_one({
        "patient_id": patient_id,
        "AgeCategory": row['AgeCategory'],
        "Sex": row['Sex'],
        "Race": row['Race']
    })
    
    # Collection health_indicators
    health_indicators_collection.insert_one({
        "patient_id": patient_id,
        "HeartDisease": row['HeartDisease'],
        "BMI": row['BMI'],
        "SleepTime": row['SleepTime'],
        "PhysicalActivity": row['PhysicalActivity'],
        "GenHealth": row['GenHealth']
    })
    
    # Collection lifestyle
    lifestyle_collection.insert_one({
        "patient_id": patient_id,
        "Smoking": row['Smoking'],
        "AlcoholDrinking": row['AlcoholDrinking'],
        "PhysicalHealth": row['PhysicalHealth'],
        "MentalHealth": row['MentalHealth']
    })

print(f"Đã nhập {patients_collection.count_documents({})} bản ghi vào mỗi collection!")

# Tạo index (sửa lỗi)
db.patients.create_index([("patient_id", 1)])
db.health_indicators.create_index([("patient_id", 1), ("HeartDisease", 1)])
db.lifestyle.create_index([("patient_id", 1)])

# Hàm thực hiện CRUD
def perform_crud():
    st.subheader("Thao tác CRUD")

    # Create
    if st.button("Thêm bệnh nhân mới"):
        new_patient_id = str(uuid.uuid4())
        patients_collection.insert_one({
            "patient_id": new_patient_id,
            "AgeCategory": "40-44",
            "Sex": "Male",
            "Race": "White"
        })
        health_indicators_collection.insert_one({
            "patient_id": new_patient_id,
            "HeartDisease": "No",
            "BMI": 27.5,
            "SleepTime": 6,
            "PhysicalActivity": "Yes",
            "GenHealth": "Good"
        })
        lifestyle_collection.insert_one({
            "patient_id": new_patient_id,
            "Smoking": "No",
            "AlcoholDrinking": "No",
            "PhysicalHealth": 2,
            "MentalHealth": 5
        })
        st.success(f"Đã thêm bệnh nhân mới với patient_id: {new_patient_id}")

    # Read
    if st.button("Đọc thông tin bệnh nhân mới nhất"):
        patient = patients_collection.find_one(sort=[("_id", -1)])
        health = health_indicators_collection.find_one({"patient_id": patient["patient_id"]})
        lifestyle = lifestyle_collection.find_one({"patient_id": patient["patient_id"]})
        st.write("Patients:", patient)
        st.write("Health Indicators:", health)
        st.write("Lifestyle:", lifestyle)

    # Update
    if st.button("Cập nhật thông tin bệnh nhân mới nhất"):
        patient = patients_collection.find_one(sort=[("_id", -1)])
        health_indicators_collection.update_one(
            {"patient_id": patient["patient_id"]},
            {"$set": {"SleepTime": 8, "GenHealth": "Very good"}}
        )
        lifestyle_collection.update_one(
            {"patient_id": patient["patient_id"]},
            {"$set": {"Smoking": "Yes"}}
        )
        updated_health = health_indicators_collection.find_one({"patient_id": patient["patient_id"]})
        st.write("Sau khi cập nhật - Health Indicators:", updated_health)

    # Delete
    if st.button("Xóa bệnh nhân mới nhất"):
        patient = patients_collection.find_one(sort=[("_id", -1)])
        patients_collection.delete_one({"patient_id": patient["patient_id"]})
        health_indicators_collection.delete_one({"patient_id": patient["patient_id"]})
        lifestyle_collection.delete_one({"patient_id": patient["patient_id"]})
        st.success("Đã xóa bệnh nhân!")

# Hàm chạy aggregation
def run_age_heart_disease():
    pipeline = [
        {
            "$lookup": {
                "from": "health_indicators",
                "localField": "patient_id",
                "foreignField": "patient_id",
                "as": "health"
            }
        },
        {"$unwind": "$health"},
        {
            "$group": {
                "_id": "$AgeCategory",
                "total": {"$sum": 1},
                "heartDiseaseCount": {
                    "$sum": {"$cond": [{"$eq": ["$health.HeartDisease", "Yes"]}, 1, 0]}
                }
            }
        },
        {
            "$project": {
                "ageCategory": "$_id",
                "heartDiseaseRatio": {"$divide": ["$heartDiseaseCount", "$total"]},
                "total": 1,
                "_id": 0
            }
        },
        {"$sort": {"ageCategory": 1}}
    ]
    return list(db.patients.aggregate(pipeline))

def run_sleep_heart_disease():
    pipeline = [
        {
            "$group": {
                "_id": "$SleepTime",
                "total": {"$sum": 1},
                "heartDiseaseCount": {
                    "$sum": {"$cond": [{"$eq": ["$HeartDisease", "Yes"]}, 1, 0]}
                }
            }
        },
        {
            "$project": {
                "sleepTime": "$_id",
                "heartDiseaseRatio": {"$divide": ["$heartDiseaseCount", "$total"]},
                "total": 1,
                "_id": 0
            }
        },
        {"$sort": {"sleepTime": 1}}
    ]
    return list(db.health_indicators.aggregate(pipeline))

def run_lifestyle_impact():
    pipeline = [
        {
            "$lookup": {
                "from": "health_indicators",
                "localField": "patient_id",
                "foreignField": "patient_id",
                "as": "health"
            }
        },
        {"$unwind": "$health"},
        {
            "$group": {
                "_id": {
                    "Smoking": "$Smoking",
                    "PhysicalActivity": "$health.PhysicalActivity"
                },
                "total": {"$sum": 1},
                "heartDiseaseCount": {
                    "$sum": {"$cond": [{"$eq": ["$health.HeartDisease", "Yes"]}, 1, 0]}
                }
            }
        },
        {
            "$project": {
                "smoking": "$_id.Smoking",
                "physicalActivity": "$_id.PhysicalActivity",
                "heartDiseaseRatio": {"$divide": ["$heartDiseaseCount", "$total"]},
                "total": 1,
                "_id": 0
            }
        }
    ]
    return list(db.lifestyle.aggregate(pipeline))

# Giao diện Streamlit
st.title("Phân tích Dữ liệu Nhịp tim với MongoDB và Streamlit")

# Thực hiện CRUD
perform_crud()

# Phân tích 1: Tỷ lệ bệnh tim theo nhóm tuổi
st.header("Tỷ lệ bệnh tim theo nhóm tuổi")
age_data = run_age_heart_disease()
df_age = pd.DataFrame(age_data)
fig, ax = plt.subplots(figsize=(10, 6))
sns.barplot(x='heartDiseaseRatio', y='ageCategory', data=df_age, ax=ax, palette='Blues')
ax.set_xlabel("Tỷ lệ bệnh tim")
ax.set_ylabel("Nhóm tuổi")
st.pyplot(fig)
st.write("**Insight**: Người lớn tuổi (65+) có tỷ lệ bệnh tim cao hơn, có thể do lão hóa.")

# Phân tích 2: Ảnh hưởng của giấc ngủ
st.header("Tỷ lệ bệnh tim theo số giờ ngủ")
sleep_data = run_sleep_heart_disease()
df_sleep = pd.DataFrame(sleep_data)
fig, ax = plt.subplots(figsize=(10, 6))
sns.lineplot(x='sleepTime', y='heartDiseaseRatio', data=df_sleep, ax=ax, marker='o', color='green')
ax.set_xlabel("Số giờ ngủ")
ax.set_ylabel("Tỷ lệ bệnh tim")
st.pyplot(fig)
st.write("**Insight**: Ngủ 7-8 giờ có tỷ lệ bệnh tim thấp nhất. Ngủ quá ít hoặc quá nhiều làm tăng nguy cơ.")

# Phân tích 3: Ảnh hưởng của lối sống
st.header("Ảnh hưởng của hút thuốc và hoạt động thể chất")
lifestyle_data = run_lifestyle_impact()
df_lifestyle = pd.DataFrame(lifestyle_data)
fig, ax = plt.subplots(figsize=(10, 6))
sns.barplot(x='heartDiseaseRatio', y='smoking', hue='physicalActivity', data=df_lifestyle, ax=ax, palette='Set2')
ax.set_xlabel("Tỷ lệ bệnh tim")
ax.set_ylabel("Hút thuốc")
st.pyplot(fig)
st.write("**Insight**: Người hút thuốc và ít vận động có nguy cơ bệnh tim cao hơn đáng kể.")