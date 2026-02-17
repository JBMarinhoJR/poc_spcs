import streamlit as st

st.set_page_config(page_title="SPCS Streamlit POC", layout="wide")

st.title("SPCS Streamlit POC (Internal)")
st.write("This Streamlit app is running inside Snowpark Container Services.")

name = st.text_input("Your name", "Data Architect")
st.success(f"Hello, {name} 👋")
