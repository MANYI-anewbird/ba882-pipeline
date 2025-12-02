import streamlit as st

# Configure page
st.set_page_config(
    page_title="GitHub Analytics Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Home page
st.title("📊 GitHub Repository Analytics Dashboard")
st.markdown("---")

st.markdown("""
## Welcome to the GitHub Analytics Platform

This dashboard provides comprehensive insights into GitHub repositories through multiple analytical lenses:

### 📈 **Repository Overview**
Explore repository metrics, age distributions, clustering patterns, commit activity, and language ecosystems.

### 🔍 **Repository Deep Dive**
Detailed analysis of individual repositories including README quality, activity timelines, and contributor patterns.

### 🤖 **ML Cluster Analysis**
Machine learning-powered clustering to identify repository patterns and characteristics.

---

### 📊 **Current Data Summary**
- **Repositories Analyzed:** 300+ top GitHub repos
- **ML Clusters Identified:** 4 distinct patterns
- **Data Sources:** Repos, Commits, Contributors, READMEs, Languages

---

👈 **Select a page from the sidebar to begin exploring!**
""")# Updated Tue Nov 11 05:28:09 PM UTC 2025
