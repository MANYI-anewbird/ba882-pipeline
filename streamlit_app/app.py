import streamlit as st
from google.cloud import bigquery
import pandas as pd
import plotly.express as px

# Configure page
st.set_page_config(page_title="GitHub Data Dashboard", layout="wide")
st.title("📊 GitHub Repository Analytics")

# Connect to BigQuery
@st.cache_resource
def get_bigquery_client():
    return bigquery.Client(project="ba-882-fall25-team8")

client = get_bigquery_client()

# Query repos data
@st.cache_data(ttl=3600)
def load_repos_data():
    query = """
    SELECT 
        full_name,
        stargazers_count,
        language,
        forks_count,
        open_issues_count,
        snapshot_date
    FROM `ba-882-fall25-team8.raw_github_data.github_repos_raw`
    WHERE snapshot_date = (SELECT MAX(snapshot_date) 
                          FROM `ba-882-fall25-team8.raw_github_data.github_repos_raw`)
    ORDER BY stargazers_count DESC
    LIMIT 100
    """
    return client.query(query).to_dataframe()

# Load data
try:
    df = load_repos_data()
    
    # Sidebar filters
    st.sidebar.header("🔍 Filters")
    languages = ['All'] + sorted(df['language'].dropna().unique().tolist())
    selected_language = st.sidebar.selectbox("Programming Language", languages)
    
    # Filter data
    if selected_language != 'All':
        filtered_df = df[df['language'] == selected_language]
    else:
        filtered_df = df
    
    # Metrics row
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Total Repos", len(filtered_df))
    with col2:
        st.metric("Avg Stars", f"{filtered_df['stargazers_count'].mean():.0f}")
    with col3:
        st.metric("Avg Forks", f"{filtered_df['forks_count'].mean():.0f}")
    with col4:
        st.metric("Total Issues", f"{filtered_df['open_issues_count'].sum():.0f}")
    
    # Visualization 1: Top repos bar chart
    st.header("⭐ Top 20 Repositories by Stars")
    top_20 = filtered_df.head(20)
    fig1 = px.bar(top_20, 
                  x='stargazers_count', 
                  y='full_name',
                  orientation='h',
                  title='',
                  labels={'stargazers_count': 'Stars', 'full_name': 'Repository'})
    fig1.update_layout(height=600, yaxis={'categoryorder':'total ascending'})
    st.plotly_chart(fig1, use_container_width=True)
    
    # Visualization 2: Language distribution
    col1, col2 = st.columns(2)
    
    with col1:
        st.header("💻 Language Distribution")
        lang_counts = df['language'].value_counts().head(10)
        fig2 = px.pie(values=lang_counts.values, 
                     names=lang_counts.index,
                     title='Top 10 Programming Languages')
        st.plotly_chart(fig2, use_container_width=True)
    
    with col2:
        st.header("🍴 Stars vs Forks")
        fig3 = px.scatter(filtered_df.head(50), 
                         x='stargazers_count', 
                         y='forks_count',
                         hover_data=['full_name'],
                         title='Correlation between Stars and Forks')
        st.plotly_chart(fig3, use_container_width=True)
    
    # Data table
    st.header("📋 Repository Details")
    st.dataframe(
        filtered_df[['full_name', 'stargazers_count', 'forks_count', 'language', 'open_issues_count']],
        use_container_width=True
    )

except Exception as e:
    st.error(f"Error loading data: {str(e)}")
    st.info("Make sure your BigQuery tables are populated with data.")
